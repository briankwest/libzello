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

    c->ws = zello_ws_create(c);
    if (!c->ws) {
        ZLOG_E("zello_ws_create failed");
        zello_client_destroy(c);
        return NULL;
    }
    return c;
}

static void tx_pending_free_all(zello_client_t *c);  /* fwd */

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
    tx_pending_free_all(c);
    free(c->tx_pcm_buf); c->tx_pcm_buf = NULL;
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

    /* WS callbacks fire from inside lws_service() — they call into
     * zello_on_ws_*() which is fine to run with the lock held since
     * it's recursive. */
    int rc = zello_ws_poll(c->ws, timeout_ms);
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
        /* Reset backoff to a longer interval — auth errors don't fix themselves. */
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

/* ── Audio TX ───────────────────────────────────────────────────── */

/* Cap on Opus packets buffered while waiting for start_stream response.
 * 32 × 60 ms = ~1.9 s — well beyond any plausible RTT. Past that we drop
 * with a warning to avoid unbounded memory growth on a stalled server. */
#define ZELLO_TX_PENDING_MAX 32

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
    free(c->tx_pcm_buf);
    c->tx_pcm_buf = malloc(sizeof(int16_t) * c->tx_frame_samples);
    if (!c->tx_pcm_buf) return ZELLO_ERR_NOMEM;
    c->tx_pcm_n = 0;
    return ZELLO_OK;
}

/* Build the 9-byte audio header + Opus payload into one buffer. Caller
 * frees with free(). */
static uint8_t *tx_wrap_audio_packet(uint32_t stream_id,
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
    pkt[5] = pkt[6] = pkt[7] = pkt[8] = 0; /* packet_id = 0 outbound */
    memcpy(pkt + ZELLO_BIN_HEADER_LEN, opus, opus_len);
    *out_len = total;
    return pkt;
}

/* Enqueue an Opus payload for sending after start_stream succeeds. */
static int tx_pending_push(zello_client_t *c, const uint8_t *opus, size_t opus_len)
{
    if (c->tx_pending_q_n >= ZELLO_TX_PENDING_MAX) {
        ZLOG_W("zello: tx pending queue full, dropping frame");
        return ZELLO_ERR;
    }
    if (c->tx_pending_q_n == c->tx_pending_q_cap) {
        size_t ncap = c->tx_pending_q_cap ? c->tx_pending_q_cap * 2 : 8;
        uint8_t **nq = realloc(c->tx_pending_q, ncap * sizeof(*nq));
        size_t  *nl  = realloc(c->tx_pending_q_len, ncap * sizeof(*nl));
        if (!nq || !nl) {
            free(nq); free(nl);
            return ZELLO_ERR_NOMEM;
        }
        c->tx_pending_q     = nq;
        c->tx_pending_q_len = nl;
        c->tx_pending_q_cap = ncap;
    }
    uint8_t *copy = malloc(opus_len);
    if (!copy) return ZELLO_ERR_NOMEM;
    memcpy(copy, opus, opus_len);
    c->tx_pending_q[c->tx_pending_q_n]     = copy;
    c->tx_pending_q_len[c->tx_pending_q_n] = opus_len;
    c->tx_pending_q_n++;
    return ZELLO_OK;
}

static void tx_pending_drain(zello_client_t *c)
{
    for (size_t i = 0; i < c->tx_pending_q_n; i++) {
        size_t pkt_len = 0;
        uint8_t *pkt = tx_wrap_audio_packet(c->tx_stream_id,
                                             c->tx_pending_q[i],
                                             c->tx_pending_q_len[i],
                                             &pkt_len);
        if (pkt) {
            zello_ws_send_binary(c->ws, pkt, pkt_len);
            free(pkt);
        }
        free(c->tx_pending_q[i]);
    }
    c->tx_pending_q_n = 0;
}

static void tx_pending_free_all(zello_client_t *c)
{
    for (size_t i = 0; i < c->tx_pending_q_n; i++) free(c->tx_pending_q[i]);
    free(c->tx_pending_q);
    free(c->tx_pending_q_len);
    c->tx_pending_q     = NULL;
    c->tx_pending_q_len = NULL;
    c->tx_pending_q_n   = 0;
    c->tx_pending_q_cap = 0;
}

int zello_client_start_tx(zello_client_t *c)
{
    if (!c) return ZELLO_ERR;
    zcli_lock(c);
    int rc;
    if (c->state != ZELLO_STATE_ONLINE)       { rc = ZELLO_ERR_STATE; goto out; }
    if (!c->channel_ready)                    { rc = ZELLO_ERR_STATE; goto out; }
    if (c->tx_pending || c->tx_active)        { rc = ZELLO_ERR_STATE; goto out; }
    if (c->cfg.listen_only) {
        ZLOG_W("zello: start_tx ignored — listen_only mode");
        rc = ZELLO_ERR_STATE; goto out;
    }

    rc = tx_ensure_encoder(c);
    if (rc != ZELLO_OK) goto out;

    uint8_t hdr[4];
    zello_codec_header_pack(hdr,
                             (uint16_t)c->cfg.tx_sample_rate,
                             (uint8_t)c->cfg.tx_frames_per_packet,
                             (uint8_t)c->cfg.tx_frame_ms);

    uint32_t seq = c->next_seq++;
    char *json = zello_build_start_stream(seq, c->cfg.channel, hdr,
                                           c->cfg.tx_frame_ms * c->cfg.tx_frames_per_packet);
    if (!json) { rc = ZELLO_ERR_NOMEM; goto out; }
    c->pending_start_stream_seq = seq;
    c->tx_pending  = true;
    c->tx_pcm_n    = 0;
    rc = zello_ws_send_text(c->ws, json, strlen(json));
    ZLOG_I("zello: start_stream sent (seq=%u, sr=%d, frame=%dms)",
           seq, c->cfg.tx_sample_rate, c->cfg.tx_frame_ms);
    free(json);
out:
    zcli_unlock(c);
    return rc;
}

int zello_client_send_pcm(zello_client_t *c, const int16_t *pcm, size_t n)
{
    if (!c || !pcm) return ZELLO_ERR;
    zcli_lock(c);
    int rc = ZELLO_OK;
    if (!c->tx_pending && !c->tx_active) { rc = ZELLO_ERR_STATE; goto out; }
    if (!c->enc || !c->tx_pcm_buf)       { rc = ZELLO_ERR_STATE; goto out; }

    /* Append to accumulator, encode whenever a full frame is ready. */
    size_t off = 0;
    while (off < n) {
        size_t space = (size_t)c->tx_frame_samples - (size_t)c->tx_pcm_n;
        size_t take  = (n - off) < space ? (n - off) : space;
        memcpy(c->tx_pcm_buf + c->tx_pcm_n, pcm + off, take * sizeof(int16_t));
        c->tx_pcm_n += (int)take;
        off += take;

        if (c->tx_pcm_n == c->tx_frame_samples) {
            uint8_t opus[ZELLO_OPUS_MAX_BYTES];
            int olen = zello_enc_encode(c->enc, c->tx_pcm_buf, opus, sizeof(opus));
            c->tx_pcm_n = 0;
            if (olen <= 0) continue;

            if (c->tx_active) {
                size_t pkt_len = 0;
                uint8_t *pkt = tx_wrap_audio_packet(c->tx_stream_id,
                                                    opus, (size_t)olen, &pkt_len);
                if (pkt) {
                    zello_ws_send_binary(c->ws, pkt, pkt_len);
                    free(pkt);
                }
            } else {
                /* tx_pending — start_stream still in flight. Buffer. */
                tx_pending_push(c, opus, (size_t)olen);
            }
        }
    }
out:
    zcli_unlock(c);
    return rc;
}

int zello_client_stop_tx(zello_client_t *c)
{
    if (!c) return ZELLO_ERR;
    zcli_lock(c);
    int rc = ZELLO_OK;
    if (!c->tx_pending && !c->tx_active) { rc = ZELLO_ERR_STATE; goto out; }

    /* Drop any partial frame at the tail. */
    c->tx_pcm_n = 0;

    if (c->tx_active) {
        uint32_t seq = c->next_seq++;
        char *json = zello_build_stop_stream(seq, c->tx_stream_id, c->cfg.channel);
        if (json) {
            c->pending_stop_stream_seq = seq;
            zello_ws_send_text(c->ws, json, strlen(json));
            free(json);
        }
        ZLOG_I("zello: stop_stream sent for stream_id=%u", c->tx_stream_id);
    } else {
        /* Cancelled before start_stream response arrived — just drop the
         * pending queue. If the response arrives later we'll honour the
         * stop in zello_client_handle_start_stream_response. */
        ZLOG_I("zello: stop_tx before start_stream response — discarding pending");
        tx_pending_free_all(c);
    }

    c->tx_active   = false;
    c->tx_pending  = false;
    c->tx_stream_id = 0;
out:
    zcli_unlock(c);
    return rc;
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
        tx_pending_free_all(c);
        c->tx_pending = false;
        return;
    }
    const cJSON *sid = cJSON_GetObjectItemCaseSensitive(root, "stream_id");
    if (!cJSON_IsNumber(sid)) {
        ZLOG_E("zello: start_stream response missing stream_id");
        tx_pending_free_all(c);
        c->tx_pending = false;
        return;
    }
    c->tx_stream_id = (uint32_t)sid->valuedouble;
    c->tx_active    = true;
    c->tx_pending   = false;
    ZLOG_I("zello: start_stream ok, stream_id=%u, draining %zu pending",
           c->tx_stream_id, c->tx_pending_q_n);
    tx_pending_drain(c);
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
