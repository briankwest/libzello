/*
 * zello_client.c — Public client API and lifecycle state machine.
 *
 * State transitions:
 *   OFFLINE   →  CONNECTING  (zello_client_start)
 *   CONNECTING → LOGON       (LWS_CALLBACK_CLIENT_ESTABLISHED)
 *   LOGON     →  ONLINE      (logon response success)
 *   ONLINE    →  RECONNECT   (WS closed / error)
 *   RECONNECT →  CONNECTING  (backoff elapsed)
 *
 * Reconnect is driven from zello_client_poll() — the polling thread
 * checks the wall clock and schedules a new connect when the backoff
 * has elapsed. No separate timer thread.
 */

#include "libzello/zello_client.h"
#include "libzello/zello_proto.h"
#include "libzello/version.h"
#include "zello_internal.h"
#include "zello_ws.h"
#include "zello_codec.h"
#include "zello_log.h"
#include "zello_proto_priv.h"

#include <cjson/cJSON.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* PCM ring capacity in int16 samples (~2 s @ 16 kHz mono). */
#define ZELLO_TX_RING_SAMPLES 32000

/* Forward decls for the TX state machine — defined further down. */
static void          tx_service(zello_client_t *c);
static inline size_t tx_ring_avail(zello_client_t *c);

/* ── utilities ──────────────────────────────────────────────────── */

static char *dup_or_null(const char *s)
{
    return s ? strdup(s) : NULL;
}

long zello_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

const char *zello_version_string(void)
{
    return LIBZELLO_VERSION_STRING;
}

/* ── helpers ────────────────────────────────────────────────────── */

static int send_logon(zello_client_t *c)
{
    uint32_t seq = c->next_seq++;
    char *json = zello_build_logon(seq,
                                    c->cfg.username,
                                    c->cfg.password,
                                    c->cfg.channel,
                                    c->cfg.auth_token,
                                    c->refresh_token,
                                    c->cfg.listen_only);
    if (!json) {
        ZLOG_E("zello: build_logon failed");
        return ZELLO_ERR_NOMEM;
    }
    c->pending_logon_seq = seq;
    int rc = zello_ws_send_text(c->ws, json, strlen(json));
    /* Don't log the raw JSON — it contains password and auth_token.
     * ZELLO_LOG_LOGON_UNSAFE=1 dumps the JSON for protocol debugging. */
    ZLOG_D("zello: sent logon (seq=%u, %zu bytes, refresh=%s, listen_only=%d)",
           seq, strlen(json),
           c->refresh_token ? "yes" : "no",
           c->cfg.listen_only ? 1 : 0);
    const char *unsafe = getenv("ZELLO_LOG_LOGON_UNSAFE");
    if (unsafe && *unsafe == '1') {
        ZLOG_W("zello: LOGON JSON (UNSAFE): %s", json);
    }
    free(json);
    return rc;
}

static void schedule_reconnect(zello_client_t *c)
{
    if (c->backoff_ms <= 0) c->backoff_ms = c->cfg.reconnect_initial_ms;
    if (c->backoff_ms > c->cfg.reconnect_max_ms) c->backoff_ms = c->cfg.reconnect_max_ms;
    c->next_reconnect_at_ms = zello_now_ms() + c->backoff_ms;
    c->state = ZELLO_STATE_RECONNECT;
    ZLOG_I("zello: reconnect scheduled in %d ms", c->backoff_ms);
    /* Double for next time, capped. */
    c->backoff_ms = c->backoff_ms < c->cfg.reconnect_max_ms / 2
                    ? c->backoff_ms * 2 : c->cfg.reconnect_max_ms;
}

/* ── lifecycle ──────────────────────────────────────────────────── */

zello_client_t *zello_client_create(const zello_config_t *cfg,
                                     const zello_callbacks_t *cb)
{
    if (!cfg || !cb) return NULL;
    if (!cfg->username || !cfg->channel) {
        ZLOG_E("zello_client_create: username and channel are required");
        return NULL;
    }

    zello_client_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->cfg.server_url           = dup_or_null(cfg->server_url ? cfg->server_url
                                                              : ZELLO_DEFAULT_SERVER_FF);
    c->cfg.username             = dup_or_null(cfg->username);
    c->cfg.password             = dup_or_null(cfg->password);
    c->cfg.channel              = dup_or_null(cfg->channel);
    c->cfg.auth_token           = dup_or_null(cfg->auth_token);
    c->cfg.listen_only          = cfg->listen_only;
    c->cfg.reconnect_initial_ms = cfg->reconnect_initial_ms > 0 ? cfg->reconnect_initial_ms : 1000;
    c->cfg.reconnect_max_ms     = cfg->reconnect_max_ms     > 0 ? cfg->reconnect_max_ms     : 60000;
    c->cfg.tx_sample_rate       = cfg->tx_sample_rate       > 0 ? cfg->tx_sample_rate       : ZELLO_DEFAULT_SAMPLE_RATE;
    c->cfg.tx_frame_ms          = cfg->tx_frame_ms          > 0 ? cfg->tx_frame_ms          : ZELLO_DEFAULT_FRAME_MS;
    c->cfg.tx_frames_per_packet = cfg->tx_frames_per_packet > 0 ? cfg->tx_frames_per_packet : ZELLO_DEFAULT_FRAMES_PER_PKT;
    c->cfg.tx_bitrate           = cfg->tx_bitrate           > 0 ? cfg->tx_bitrate           : 24000;

    c->cb         = *cb;
    c->state      = ZELLO_STATE_OFFLINE;
    c->next_seq   = 1;
    c->backoff_ms = c->cfg.reconnect_initial_ms;

    /* Recursive so callbacks (which fire while we hold the lock from
     * zello_client_poll) can re-enter libzello without deadlock. */
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&c->mtx, &attr);
    pthread_mutexattr_destroy(&attr);

    /* SPSC PCM ring (~2 s @ 16 kHz). Lock-free from the caller's side. */
    c->tx_ring_cap = ZELLO_TX_RING_SAMPLES;
    c->tx_ring     = calloc(c->tx_ring_cap, sizeof(int16_t));
    if (!c->tx_ring) {
        ZLOG_E("tx_ring alloc failed");
        zello_client_destroy(c);
        return NULL;
    }
    atomic_init(&c->tx_ring_w,    0);
    atomic_init(&c->tx_ring_r,    0);
    atomic_init(&c->tx_req_start, 0);
    atomic_init(&c->tx_req_stop,  0);
    atomic_init(&c->tx_streaming, 0);

    c->ws = zello_ws_create(c);
    if (!c->ws) {
        ZLOG_E("zello_ws_create failed");
        zello_client_destroy(c);
        return NULL;
    }
    return c;
}

void zello_client_destroy(zello_client_t *c)
{
    if (!c) return;
    /* Tear down the WS first (any pending callbacks finish under lock),
     * then drop the lock and free everything else. We don't hold the
     * mutex while destroying the mutex itself. */
    zcli_lock(c);
    if (c->ws)  { zello_ws_destroy(c->ws);  c->ws  = NULL; }
    if (c->enc) { zello_enc_destroy(c->enc); c->enc = NULL; }
    if (c->dec) { zello_dec_destroy(c->dec); c->dec = NULL; }
    free(c->tx_ring);      c->tx_ring      = NULL;
    free(c->tx_frame_buf); c->tx_frame_buf = NULL;
    zcli_unlock(c);

    pthread_mutex_destroy(&c->mtx);

    free(c->cfg.server_url);
    free(c->cfg.username);
    free(c->cfg.password);
    free(c->cfg.channel);
    free(c->cfg.auth_token);
    free(c->refresh_token);
    free(c);
}

int zello_client_start(zello_client_t *c)
{
    if (!c) return ZELLO_ERR;
    zcli_lock(c);
    int rc;
    if (c->state != ZELLO_STATE_OFFLINE && c->state != ZELLO_STATE_RECONNECT) {
        rc = ZELLO_ERR_STATE;
    } else {
        ZLOG_I("zello_client_start: connecting to %s (channel=%s)",
               c->cfg.server_url, c->cfg.channel);
        c->state = ZELLO_STATE_CONNECTING;
        rc = zello_ws_connect(c->ws, c->cfg.server_url);
        if (rc != ZELLO_OK) schedule_reconnect(c);
    }
    zcli_unlock(c);
    return rc;
}

int zello_client_stop(zello_client_t *c)
{
    if (!c) return ZELLO_ERR;
    zcli_lock(c);
    zello_ws_close(c->ws);
    c->state = ZELLO_STATE_OFFLINE;
    zcli_unlock(c);
    return ZELLO_OK;
}

int zello_client_poll(zello_client_t *c, int timeout_ms)
{
    if (!c) return ZELLO_ERR;
    zcli_lock(c);

    /* If we're waiting on a reconnect, see if it's time to try again. */
    if (c->state == ZELLO_STATE_RECONNECT) {
        if (zello_now_ms() >= c->next_reconnect_at_ms) {
            ZLOG_I("zello: reconnect attempt");
            c->state = ZELLO_STATE_CONNECTING;
            zello_ws_connect(c->ws, c->cfg.server_url);
        }
    }

    /* TX state machine — drive start/stop requests, drain the SPSC
     * PCM ring at frame-ms cadence. */
    tx_service(c);

    /* Cap the WS poll timeout while a TX is active so the service
     * thread comes back in time for the next drain slot. Without this
     * cap, a caller that requested e.g. timeout_ms=100 would block in
     * poll() for the full 100 ms after a drain, then drain one more
     * frame — net drain rate ≈ 1 frame per 100 ms = 9.6 kHz, well
     * under the producer's 16 kHz. Ring grows unbounded, listeners
     * hear audio arriving late and Opus PLC fills the gaps with
     * static. Time-until-next-drain is always ≤ frame_ms while
     * active. */
    int effective_timeout = timeout_ms;
    if (c->tx_active && c->cfg.tx_frame_ms > 0) {
        long now      = zello_now_ms();
        long until_ms = c->tx_next_encode_ms - now;
        if (until_ms < 0) until_ms = 0;
        if (until_ms > c->cfg.tx_frame_ms) until_ms = c->cfg.tx_frame_ms;
        /* If we're "due" but the ring doesn't have a full frame yet,
         * sleep a little anyway instead of spinning — producer pushes
         * every ~20 ms, so half that gives it a turn without starving
         * the drain. */
        if (until_ms == 0 &&
            tx_ring_avail(c) < (size_t)c->tx_frame_samples)
            until_ms = 10;
        if (effective_timeout > (int)until_ms) effective_timeout = (int)until_ms;
    }

    /* WS callbacks fire from inside zello_ws_poll — they call into
     * zello_on_ws_*() which is fine to run with the lock held since
     * the mutex is recursive. */
    int rc = zello_ws_poll(c->ws, effective_timeout);
    zcli_unlock(c);
    return rc;
}

zello_state_t zello_client_state(const zello_client_t *c)
{
    /* `state` is a single enum word — atomic on every arch we target.
     * We deliberately don't take the lock here so callers can poll
     * state from any thread without contention. */
    return c ? c->state : ZELLO_STATE_OFFLINE;
}

/* ── WS → client event handlers (called from zello_ws.c) ────────── */

void zello_on_ws_established(zello_client_t *c)
{
    if (!c) return;
    ZLOG_I("zello: WS established, sending logon");
    c->state = ZELLO_STATE_LOGON;
    send_logon(c);
}

void zello_on_ws_error(zello_client_t *c, const char *reason)
{
    if (!c) return;
    ZLOG_W("zello: WS error: %s", reason ? reason : "(none)");
    c->channel_ready = false;
    c->tx_active = c->tx_pending = false;
    if (c->cb.on_disconnected)
        c->cb.on_disconnected(c, -1, reason, c->cb.userdata);
    schedule_reconnect(c);
}

void zello_on_ws_closed(zello_client_t *c)
{
    if (!c) return;
    ZLOG_I("zello: WS closed");
    c->channel_ready = false;
    c->tx_active = c->tx_pending = false;
    if (c->cb.on_disconnected)
        c->cb.on_disconnected(c, 0, "closed", c->cb.userdata);
    schedule_reconnect(c);
}

/* Max samples one Opus packet can produce: 120 ms @ 48 kHz = 5760. */
#define ZELLO_RX_MAX_SAMPLES 5760

static void handle_binary_audio(zello_client_t *c, const uint8_t *buf, size_t len)
{
    if (len < ZELLO_BIN_HEADER_LEN) {
        ZLOG_W("zello: binary frame too short (%zu bytes)", len);
        return;
    }
    if (buf[0] != ZELLO_PKT_AUDIO) {
        /* Image (0x02) and any unknown types are ignored in v0.1. */
        ZLOG_D("zello: binary type=0x%02x ignored (%zu bytes)", buf[0], len);
        return;
    }

    uint32_t stream_id =
        ((uint32_t)buf[1] << 24) | ((uint32_t)buf[2] << 16) |
        ((uint32_t)buf[3] <<  8) | ((uint32_t)buf[4]);
    /* packet_id (buf[5..8]) is informational — we don't need it for decoding. */
    const uint8_t *opus = buf + ZELLO_BIN_HEADER_LEN;
    size_t opus_len = len - ZELLO_BIN_HEADER_LEN;

    if (!c->rx_active || stream_id != c->rx_stream_id) {
        ZLOG_D("zello: audio for unknown stream_id=%u (active=%u), dropping",
               stream_id, c->rx_active ? c->rx_stream_id : 0);
        return;
    }
    if (!c->dec) {
        ZLOG_W("zello: audio frame arrived before decoder created");
        return;
    }

    int16_t pcm[ZELLO_RX_MAX_SAMPLES];
    int n = zello_dec_decode(c->dec, opus, opus_len, pcm, ZELLO_RX_MAX_SAMPLES);
    if (n < 0) return;
    ZLOG_D("zello: rx audio sid=%u opus=%zu B pcm=%d samples @ %d Hz",
           stream_id, opus_len, n, c->rx_sample_rate);
    if (c->cb.on_audio)
        c->cb.on_audio(c, stream_id, pcm, (size_t)n, c->rx_sample_rate, c->cb.userdata);
}

void zello_on_ws_message(zello_client_t *c, bool binary,
                          const uint8_t *buf, size_t len)
{
    if (!c || !buf) return;

    if (binary) {
        handle_binary_audio(c, buf, len);
        return;
    }

    /* Parse JSON. cJSON_ParseWithLength is safer than cJSON_Parse. */
    cJSON *root = cJSON_ParseWithLength((const char *)buf, len);
    if (!root) {
        ZLOG_W("zello: malformed JSON (%zu bytes)", len);
        return;
    }
    zello_dispatch_message(c, root);
    cJSON_Delete(root);
}

/* ── Protocol-level handlers called from zello_dispatch_message ── */

void zello_client_handle_logon_response(zello_client_t *c, const cJSON *root)
{
    const cJSON *err = cJSON_GetObjectItemCaseSensitive(root, "error");
    if (cJSON_IsString(err)) {
        ZLOG_E("zello: logon failed: %s", err->valuestring);
        if (c->cb.on_error) c->cb.on_error(c, err->valuestring, c->cb.userdata);
        /* When the failure was on a refresh-token logon, throw the
         * refresh_token away so the next reconnect falls back to a
         * fresh username+password logon. Common case: the user logged
         * in elsewhere with the same account, the server kicked us,
         * and the refresh_token we cached is no longer valid — server
         * responds "not authorized" / "no permission" indefinitely
         * unless we re-auth from scratch. */
        if (c->refresh_token) {
            ZLOG_I("zello: clearing stale refresh_token after logon error — "
                   "next reconnect will use username+password");
            free(c->refresh_token);
            c->refresh_token = NULL;
        }
        return;
    }

    const cJSON *rt = cJSON_GetObjectItemCaseSensitive(root, "refresh_token");
    if (cJSON_IsString(rt)) {
        free(c->refresh_token);
        c->refresh_token = strdup(rt->valuestring);
        ZLOG_I("zello: logon ok, refresh_token captured (%zu bytes)",
               strlen(c->refresh_token));
    } else {
        ZLOG_I("zello: logon ok (no refresh_token in response)");
    }

    c->state = ZELLO_STATE_ONLINE;
    c->backoff_ms = c->cfg.reconnect_initial_ms; /* reset for next disconnect */
    /* Note: on_connected is deferred until on_channel_status reports
     * the channel online — start_stream is rejected until then. */
}

void zello_client_handle_on_stream_start(zello_client_t *c, const cJSON *root)
{
    /* Decode codec_header to surface sample_rate / frame_ms to caller. */
    const cJSON *sid_v = cJSON_GetObjectItemCaseSensitive(root, "stream_id");
    const cJSON *hdr_v = cJSON_GetObjectItemCaseSensitive(root, "codec_header");
    const cJSON *from_v = cJSON_GetObjectItemCaseSensitive(root, "from");

    uint32_t sid = cJSON_IsNumber(sid_v) ? (uint32_t)sid_v->valuedouble : 0;
    int sample_rate = ZELLO_DEFAULT_SAMPLE_RATE;
    int frame_ms    = ZELLO_DEFAULT_FRAME_MS;

    if (cJSON_IsString(hdr_v)) {
        uint8_t raw[4] = {0};
        size_t got = 0;
        if (zello_b64_decode(hdr_v->valuestring, raw, sizeof(raw), &got) == ZELLO_OK
            && got == 4) {
            uint16_t sr = 0;
            uint8_t fpp = 0, fms = 0;
            zello_codec_header_unpack(raw, &sr, &fpp, &fms);
            sample_rate = sr;
            frame_ms    = fms;
        }
    }

    ZLOG_I("zello: stream %u from=%s sr=%d frame=%dms",
           sid, cJSON_IsString(from_v) ? from_v->valuestring : "?",
           sample_rate, frame_ms);

    /* Build/refresh the decoder for this stream's sample rate. */
    if (c->dec && c->rx_sample_rate != sample_rate) {
        zello_dec_destroy(c->dec);
        c->dec = NULL;
    }
    if (!c->dec) {
        c->dec = zello_dec_create(sample_rate);
        if (!c->dec) {
            ZLOG_E("zello: failed to create Opus decoder for sr=%d", sample_rate);
            return;
        }
    }
    c->rx_stream_id   = sid;
    c->rx_sample_rate = sample_rate;
    c->rx_active      = true;

    if (c->cb.on_stream_start) {
        c->cb.on_stream_start(c, sid,
                              cJSON_IsString(from_v) ? from_v->valuestring : NULL,
                              sample_rate, frame_ms, c->cb.userdata);
    }
}

void zello_client_handle_on_stream_stop(zello_client_t *c, const cJSON *root)
{
    const cJSON *sid_v = cJSON_GetObjectItemCaseSensitive(root, "stream_id");
    uint32_t sid = cJSON_IsNumber(sid_v) ? (uint32_t)sid_v->valuedouble : 0;
    ZLOG_I("zello: stream %u stop", sid);
    if (c->rx_active && sid == c->rx_stream_id) {
        c->rx_active = false;
        /* Keep the decoder allocated for the next stream at the same rate;
         * it's only a few KB and avoids per-PTT churn. */
    }
    if (c->cb.on_stream_stop) c->cb.on_stream_stop(c, sid, c->cb.userdata);
}

/* ── Audio TX — lock-free producer + paced consumer ─────────────── */

/* Largest Opus payload we'll emit. Spec is 1275 bytes; pad for safety. */
#define ZELLO_OPUS_MAX_BYTES 1500

static int tx_ensure_encoder(zello_client_t *c)
{
    if (c->enc) return ZELLO_OK;
    c->enc = zello_enc_create(c->cfg.tx_sample_rate,
                               c->cfg.tx_frame_ms,
                               c->cfg.tx_frames_per_packet,
                               c->cfg.tx_bitrate);
    if (!c->enc) return ZELLO_ERR_CODEC;
    c->tx_frame_samples = zello_enc_frame_samples(c->enc);
    if (c->tx_frame_samples <= 0) return ZELLO_ERR_CODEC;
    free(c->tx_frame_buf);
    c->tx_frame_buf = malloc(sizeof(int16_t) * c->tx_frame_samples);
    if (!c->tx_frame_buf) return ZELLO_ERR_NOMEM;
    return ZELLO_OK;
}

/* Build the 9-byte audio header + Opus payload into one buffer. Caller
 * frees with free().
 *
 * The published API doc says "packet_id is ignored on outbound, fill
 * with zeros" — but the official zello-channel-api JS SDK actually
 * fills packet_id with a per-stream sequence starting at 1
 * (outgoingMessage.js: ++this.currentPacketId). Listeners hearing only
 * static when we sent packet_id=0 for every packet suggests the
 * server-side path now relies on the sequence number for re-timing /
 * de-duplication, so match the SDK. */
static uint8_t *tx_wrap_audio_packet(uint32_t stream_id, uint32_t packet_id,
                                      const uint8_t *opus, size_t opus_len,
                                      size_t *out_len)
{
    size_t total = ZELLO_BIN_HEADER_LEN + opus_len;
    uint8_t *pkt = malloc(total);
    if (!pkt) return NULL;
    pkt[0] = ZELLO_PKT_AUDIO;
    pkt[1] = (uint8_t)((stream_id >> 24) & 0xFF);
    pkt[2] = (uint8_t)((stream_id >> 16) & 0xFF);
    pkt[3] = (uint8_t)((stream_id >>  8) & 0xFF);
    pkt[4] = (uint8_t)((stream_id      ) & 0xFF);
    pkt[5] = (uint8_t)((packet_id >> 24) & 0xFF);
    pkt[6] = (uint8_t)((packet_id >> 16) & 0xFF);
    pkt[7] = (uint8_t)((packet_id >>  8) & 0xFF);
    pkt[8] = (uint8_t)((packet_id      ) & 0xFF);
    memcpy(pkt + ZELLO_BIN_HEADER_LEN, opus, opus_len);
    *out_len = total;
    return pkt;
}

/* SPSC ring helpers — producer is the caller (any thread); consumer is
 * the service thread inside tx_service(). Indices are monotonically
 * increasing size_t (no wrap; the modulo happens only on access). */
static inline size_t tx_ring_avail(zello_client_t *c)
{
    size_t w = atomic_load_explicit(&c->tx_ring_w, memory_order_acquire);
    size_t r = atomic_load_explicit(&c->tx_ring_r, memory_order_relaxed);
    return w - r;
}

static void tx_ring_reset(zello_client_t *c)
{
    /* Only safe to call when no producer is racing with us — i.e. from
     * tx_service() under the client mutex, before the next start. */
    atomic_store(&c->tx_ring_r, 0);
    atomic_store(&c->tx_ring_w, 0);
}

/* Consume one full frame (or `avail` samples, zero-padded) from the
 * ring, encode, queue to lws. Called only on the service thread. */
static void tx_drain_one_frame(zello_client_t *c, int allow_partial)
{
    size_t r     = atomic_load_explicit(&c->tx_ring_r, memory_order_relaxed);
    size_t w     = atomic_load_explicit(&c->tx_ring_w, memory_order_acquire);
    size_t avail = w - r;
    if (avail == 0) return;

    int fs = c->tx_frame_samples;
    if ((int)avail < fs && !allow_partial) return;

    size_t take = (int)avail >= fs ? (size_t)fs : avail;
    for (size_t i = 0; i < take; i++)
        c->tx_frame_buf[i] = c->tx_ring[(r + i) % c->tx_ring_cap];
    for (size_t i = take; i < (size_t)fs; i++)
        c->tx_frame_buf[i] = 0;
    atomic_store_explicit(&c->tx_ring_r, r + take, memory_order_release);

    uint8_t opus[ZELLO_OPUS_MAX_BYTES];
    int olen = zello_enc_encode(c->enc, c->tx_frame_buf, opus, sizeof(opus));
    if (olen <= 0) {
        ZLOG_W("tx_drain: encode returned %d (take=%zu, fs=%d)", olen, take, fs);
        return;
    }
    c->tx_frames_sent++;
    size_t pkt_len = 0;
    /* packet_id starts at 1 for the first audio packet of the stream
     * (matches the JS SDK's currentPacketId pre-increment from 0). */
    uint8_t *pkt = tx_wrap_audio_packet(c->tx_stream_id,
                                         (uint32_t)c->tx_frames_sent,
                                         opus, (size_t)olen, &pkt_len);
    if (pkt) {
        int rc = zello_ws_send_binary(c->ws, pkt, pkt_len);
        if (rc != ZELLO_OK) {
            ZLOG_W("tx_drain: ws_send_binary failed rc=%d", rc);
        }
        free(pkt);
    } else {
        ZLOG_W("tx_drain: tx_wrap_audio_packet failed");
    }
    ZLOG_I("tx_drain: sent frame #%d (opus=%d B, ring_avail=%zu)",
           c->tx_frames_sent, olen, tx_ring_avail(c));
}

/* Driven from zello_client_poll under the client mutex. Handles:
 *   - tx_req_start: ensure encoder, send start_stream, mark pending
 *   - tx_req_stop:  drain remaining audio, send stop_stream
 *   - paced drain:  encode + send one frame per frame_ms when active
 */
static void tx_service(zello_client_t *c)
{
    /* Start request. We only clear the flag once we've actually
     * acted on it, so a request issued while we're still finishing
     * the previous stream doesn't get lost. */
    if (atomic_load(&c->tx_req_start) &&
        c->state == ZELLO_STATE_ONLINE && c->channel_ready &&
        !c->tx_pending && !c->tx_active && !c->cfg.listen_only) {
        atomic_store(&c->tx_req_start, 0);
        if (tx_ensure_encoder(c) == ZELLO_OK) {
            /* Fresh encoder state per stream. The listener gets a new
             * stream_id from the server and starts a fresh decoder; if
             * we kept residual prediction/LPC state from the previous
             * stream, the first ~100ms of audio decoded against a clean
             * decoder would come out as garbled noise. */
            zello_enc_reset(c->enc);
            uint8_t hdr[4];
            zello_codec_header_pack(hdr,
                (uint16_t)c->cfg.tx_sample_rate,
                (uint8_t)c->cfg.tx_frames_per_packet,
                (uint8_t)c->cfg.tx_frame_ms);
            uint32_t seq = c->next_seq++;
            char *json = zello_build_start_stream(seq, c->cfg.channel, hdr,
                c->cfg.tx_frame_ms * c->cfg.tx_frames_per_packet);
            if (json) {
                c->pending_start_stream_seq = seq;
                c->tx_pending = true;
                c->tx_stop_after_drain = false;
                c->tx_next_encode_ms = 0;
                c->tx_frames_sent = 0;
                zello_ws_send_text(c->ws, json, strlen(json));
                ZLOG_I("zello: start_stream sent (seq=%u, sr=%d, frame=%dms)",
                       seq, c->cfg.tx_sample_rate, c->cfg.tx_frame_ms);
                free(json);
            }
        }
    }

    /* Paced encode + send while active.
     *
     * After each drain we set tx_next_encode_ms = now + frame_ms (not
     * += frame_ms): this prevents the "catch-up burst" where, if a
     * service-thread tick was delayed, the next tick would fire two or
     * three drains back-to-back to make up for it. Zello listeners
     * apparently re-time packets based on inter-arrival rather than a
     * per-packet timestamp, so bursts come out as garbled audio on the
     * receiver. Producer is a real-time audio thread pushing samples
     * at the encoder's nominal rate, so capping consumer to that same
     * rate is the right equilibrium — back-pressure isn't a concern.
     *
     * We still allow only one frame per service iteration; ws_poll
     * comes back promptly when there's work to do, so this is
     * sufficient to keep up with the producer in steady state. */
    if (c->tx_active && c->enc && c->tx_frame_buf) {
        long now = zello_now_ms();
        if (c->tx_next_encode_ms == 0) c->tx_next_encode_ms = now;
        if (now >= c->tx_next_encode_ms &&
            tx_ring_avail(c) >= (size_t)c->tx_frame_samples) {
            tx_drain_one_frame(c, 0);
            c->tx_next_encode_ms = now + c->cfg.tx_frame_ms;
        }
    }

    /* Stop request. */
    if (atomic_load(&c->tx_req_stop)) {
        if (c->tx_active) {
            /* Keep draining at cadence until ring is empty, then send
             * stop_stream. (We process at most one full frame per
             * service iteration above; this branch flushes any partial
             * remainder once nothing more fits a whole frame.) */
            if (tx_ring_avail(c) >= (size_t)c->tx_frame_samples) {
                /* still got whole frames — wait for next pass */
                return;
            }
            /* Flush any tail samples zero-padded into a final frame so
             * the listener gets the actual end of the audio. */
            if (tx_ring_avail(c) > 0)
                tx_drain_one_frame(c, 1);

            uint32_t seq = c->next_seq++;
            char *json = zello_build_stop_stream(seq, c->tx_stream_id, c->cfg.channel);
            if (json) {
                c->pending_stop_stream_seq = seq;
                zello_ws_send_text(c->ws, json, strlen(json));
                free(json);
            }
            ZLOG_I("zello: stop_stream sent for stream_id=%u (%d frames drained)",
                   c->tx_stream_id, c->tx_frames_sent);
            c->tx_active   = false;
            c->tx_stream_id = 0;
            c->tx_frames_sent = 0;
        } else if (c->tx_pending) {
            /* Stop requested before start_stream response — let the
             * response handler send an immediate stop_stream. */
            c->tx_stop_after_drain = true;
        }
        atomic_store(&c->tx_req_stop, 0);
    }
}

/* ── Public TX API — all lock-free flag setters ─────────────────── */

int zello_client_start_tx(zello_client_t *c)
{
    if (!c) return ZELLO_ERR;
    if (c->cfg.listen_only) return ZELLO_ERR_STATE;
    /* Open the producer gate AFTER resetting the ring so a concurrent
     * send_pcm thread can't write into stale indices. The gate (zero ->
     * one) is the LAST store, with release semantics; producers do an
     * acquire load before any ring write, so by the time they observe
     * the gate open, they also observe ring_r/ring_w = 0. */
    atomic_store_explicit(&c->tx_ring_r,    0, memory_order_relaxed);
    atomic_store_explicit(&c->tx_ring_w,    0, memory_order_relaxed);
    atomic_store_explicit(&c->tx_req_stop,  0, memory_order_relaxed);
    atomic_store_explicit(&c->tx_req_start, 1, memory_order_relaxed);
    atomic_store_explicit(&c->tx_streaming, 1, memory_order_release);
    return ZELLO_OK;
}

int zello_client_send_pcm(zello_client_t *c, const int16_t *pcm, size_t n)
{
    if (!c || !pcm) return ZELLO_ERR;
    if (!c->tx_ring) return ZELLO_ERR_STATE;

    /* Acquire-load on the streaming gate. Pairs with the release-store
     * in zello_client_start_tx: if we see streaming=1, we also see the
     * ring indices reset to 0. Outside a stream we drop samples
     * silently — the caller hasn't called start_tx, or it called
     * stop_tx, and either way there's no consumer for these bytes. */
    if (atomic_load_explicit(&c->tx_streaming, memory_order_acquire) == 0)
        return ZELLO_OK;

    size_t w     = atomic_load_explicit(&c->tx_ring_w, memory_order_relaxed);
    size_t r     = atomic_load_explicit(&c->tx_ring_r, memory_order_acquire);
    size_t space = c->tx_ring_cap - (w - r);
    if (n > space) return ZELLO_ERR;   /* ring full — caller must back off */

    for (size_t i = 0; i < n; i++)
        c->tx_ring[(w + i) % c->tx_ring_cap] = pcm[i];

    atomic_store_explicit(&c->tx_ring_w, w + n, memory_order_release);
    return ZELLO_OK;
}

int zello_client_stop_tx(zello_client_t *c)
{
    if (!c) return ZELLO_ERR;
    /* Close the producer gate FIRST so concurrent send_pcm calls bail
     * out and can't slip writes into the ring after the service thread
     * has already drained it. The req_stop flag is then picked up by
     * the service thread, which flushes whatever the producer pushed
     * before observing the gate=0. */
    atomic_store_explicit(&c->tx_streaming, 0, memory_order_release);
    atomic_store(&c->tx_req_stop, 1);
    return ZELLO_OK;
}

/* Called from zello_dispatch_message when a response carries our
 * pending_start_stream_seq. */
void zello_client_handle_start_stream_response(zello_client_t *c, const cJSON *root)
{
    c->pending_start_stream_seq = 0;
    const cJSON *err = cJSON_GetObjectItemCaseSensitive(root, "error");
    if (cJSON_IsString(err)) {
        ZLOG_E("zello: start_stream failed: %s", err->valuestring);
        if (c->cb.on_error) c->cb.on_error(c, err->valuestring, c->cb.userdata);
        c->tx_pending = false;
        c->tx_stop_after_drain = false;
        tx_ring_reset(c);
        return;
    }
    const cJSON *sid = cJSON_GetObjectItemCaseSensitive(root, "stream_id");
    if (!cJSON_IsNumber(sid)) {
        ZLOG_E("zello: start_stream response missing stream_id");
        c->tx_pending = false;
        c->tx_stop_after_drain = false;
        return;
    }
    c->tx_stream_id = (uint32_t)sid->valuedouble;
    c->tx_pending   = false;

    if (c->tx_stop_after_drain) {
        c->tx_stop_after_drain = false;
        uint32_t seq = c->next_seq++;
        char *json = zello_build_stop_stream(seq, c->tx_stream_id, c->cfg.channel);
        if (json) {
            c->pending_stop_stream_seq = seq;
            zello_ws_send_text(c->ws, json, strlen(json));
            free(json);
        }
        ZLOG_I("zello: late start_stream ok (stream_id=%u) — sending immediate stop",
               c->tx_stream_id);
        c->tx_stream_id = 0;
        return;
    }

    c->tx_active         = true;
    c->tx_next_encode_ms = zello_now_ms();
    c->tx_frames_sent    = 0;
    ZLOG_I("zello: start_stream ok, stream_id=%u (%zu samples queued, fs=%d)",
           c->tx_stream_id, tx_ring_avail(c), c->tx_frame_samples);
}

int zello_client_send_text(zello_client_t *c, const char *text)
{
    if (!c || !text) return ZELLO_ERR;
    zcli_lock(c);
    int rc;
    if (c->state != ZELLO_STATE_ONLINE) { rc = ZELLO_ERR_STATE; goto out; }
    uint32_t seq = c->next_seq++;
    char *json = zello_build_text_message(seq, c->cfg.channel, text);
    if (!json) { rc = ZELLO_ERR_NOMEM; goto out; }
    rc = zello_ws_send_text(c->ws, json, strlen(json));
    free(json);
out:
    zcli_unlock(c);
    return rc;
}
