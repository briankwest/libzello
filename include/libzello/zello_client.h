/*
 * zello_client.h — Public client API for libzello.
 *
 * Polled, single-thread, one-channel client. The caller drives the event
 * loop by calling zello_client_poll() repeatedly (e.g. every 10–20 ms).
 *
 *   1. zello_client_create(cfg, cb)  → handle
 *   2. zello_client_start(h)         → initiates WS connect + logon
 *   3. loop: zello_client_poll(h, timeout_ms)
 *      callbacks fire: on_connected, on_channel_status, on_stream_*, on_audio
 *   4. zello_client_start_tx() → zello_client_send_pcm()* → zello_client_stop_tx()
 *   5. zello_client_stop(h); zello_client_destroy(h)
 *
 * No internal threads. All callbacks run on the thread that calls poll().
 */

#ifndef LIBZELLO_ZELLO_CLIENT_H
#define LIBZELLO_ZELLO_CLIENT_H

#include "zello.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zello_client zello_client_t;

/* ── Configuration ──────────────────────────────────────────────── */

typedef struct {
    /* WebSocket endpoint. NULL = "wss://zello.io/ws" (Zello Friends & Family).
     * For Zello Work, set to "wss://zellowork.io/ws/<network_name>". */
    const char *server_url;

    /* Account credentials. */
    const char *username;
    const char *password;

    /* Channel to join on logon. Required. */
    const char *channel;

    /* Developer JWT issued from developers.zello.com — REQUIRED for
     * Friends & Family logons. Ignored on Zello Work. */
    const char *auth_token;

    /* If true, send listen_only=true on logon — server will reject any
     * start_stream from us. Required for anonymous F&F logons. */
    bool listen_only;

    /* Reconnect behaviour. 0 = use defaults (1s initial, 60s cap). */
    int reconnect_initial_ms;
    int reconnect_max_ms;

    /* TX-side codec config. 0 = defaults (16 kHz, 60 ms, 1 frame/packet). */
    int tx_sample_rate;
    int tx_frame_ms;
    int tx_frames_per_packet;
    int tx_bitrate;          /* bits/s; 0 = ~24 kbps */
} zello_config_t;

/* ── Callbacks ──────────────────────────────────────────────────── */

typedef struct {
    /* WS+logon succeeded. */
    void (*on_connected)(zello_client_t *c, const char *refresh_token, void *ud);

    /* WS dropped. code is libwebsockets close code or libzello internal. */
    void (*on_disconnected)(zello_client_t *c, int code, const char *reason, void *ud);

    /* Channel join state update. */
    void (*on_channel_status)(zello_client_t *c, const char *channel,
                              bool online, int users_online, void *ud);

    /* A remote speaker started transmitting. sample_rate/frame_ms come from
     * the codec_header on this stream — caller should adapt resampling. */
    void (*on_stream_start)(zello_client_t *c, uint32_t stream_id,
                            const char *from_username,
                            int sample_rate, int frame_ms, void *ud);

    /* Decoded PCM from a remote stream. mono int16 at `sample_rate`. */
    void (*on_audio)(zello_client_t *c, uint32_t stream_id,
                     const int16_t *pcm, size_t n_samples,
                     int sample_rate, void *ud);

    /* Remote stream ended. */
    void (*on_stream_stop)(zello_client_t *c, uint32_t stream_id, void *ud);

    /* Inbound text message. */
    void (*on_text_message)(zello_client_t *c, const char *from_username,
                            const char *text, void *ud);

    /* Server-reported error (on_error JSON message). */
    void (*on_error)(zello_client_t *c, const char *code, void *ud);

    void *userdata;
} zello_callbacks_t;

/* ── Lifecycle ──────────────────────────────────────────────────── */

zello_client_t *zello_client_create(const zello_config_t *cfg,
                                     const zello_callbacks_t *cb);
int  zello_client_start  (zello_client_t *c);
int  zello_client_stop   (zello_client_t *c);
void zello_client_destroy(zello_client_t *c);

/* Service the WS event loop. Blocks at most timeout_ms. Returns 0 on
 * normal poll completion, negative on fatal error. */
int  zello_client_poll(zello_client_t *c, int timeout_ms);

zello_state_t zello_client_state(const zello_client_t *c);

/* ── Audio TX ───────────────────────────────────────────────────── */

/* Send start_stream for the bridged channel. Caller may follow with
 * zello_client_send_pcm() any number of times, then stop_tx. */
int zello_client_start_tx(zello_client_t *c);

/* Encode and send int16 mono PCM at the configured tx_sample_rate.
 * `n_samples` must be a multiple of the frame size (frame_ms * rate / 1000).
 * libzello internally pads/buffers partial frames — caller can stream any
 * amount and the buffer is flushed on stop_tx. */
int zello_client_send_pcm(zello_client_t *c,
                           const int16_t *pcm, size_t n_samples);

/* Send stop_stream for the active outbound stream. */
int zello_client_stop_tx(zello_client_t *c);

/* ── Out-of-band messages ───────────────────────────────────────── */

int zello_client_send_text(zello_client_t *c, const char *text);

/* ── Logging ────────────────────────────────────────────────────── */

typedef void (*zello_log_fn)(int level, const char *msg, void *ud);

void zello_set_log_callback(zello_log_fn fn, void *ud);
void zello_set_log_level(int level);

const char *zello_version_string(void);

#ifdef __cplusplus
}
#endif

#endif
