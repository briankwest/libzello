/*
 * zello_internal.h — Shared private state for libzello.
 *
 * Layout of struct zello_client. All members are owned by the polling
 * thread. The library is not safe for concurrent access from multiple
 * threads without external synchronization.
 */

#ifndef LIBZELLO_INTERNAL_H
#define LIBZELLO_INTERNAL_H

#include "libzello/zello.h"
#include "libzello/zello_client.h"

#include <stdint.h>

/* Forward decls — full definitions in zello_ws.h / zello_codec.h. */
struct zello_ws;
struct zello_enc;
struct zello_dec;

/* Owned copy of user-supplied config (so caller strings can be freed). */
typedef struct {
    char *server_url;
    char *username;
    char *password;
    char *channel;
    char *auth_token;
    bool  listen_only;
    int   reconnect_initial_ms;
    int   reconnect_max_ms;
    int   tx_sample_rate;
    int   tx_frame_ms;
    int   tx_frames_per_packet;
    int   tx_bitrate;
} zello_config_owned_t;

struct zello_client {
    zello_config_owned_t cfg;
    zello_callbacks_t    cb;

    zello_state_t state;

    /* WS transport (libwebsockets context + connection). */
    struct zello_ws *ws;

    /* Outbound stream — see "TX state" block below. */
    uint32_t tx_stream_id;

    /* Refresh token captured from logon response. */
    char *refresh_token;

    /* Logon sequence number bookkeeping. */
    uint32_t next_seq;
    uint32_t pending_logon_seq;
    uint32_t pending_start_stream_seq;
    uint32_t pending_stop_stream_seq;

    /* Reconnect backoff state. */
    int  backoff_ms;
    long next_reconnect_at_ms;

    /* Opus encoder/decoder — created lazily. */
    struct zello_enc *enc;
    struct zello_dec *dec;

    /* Channel readiness: set when the server emits on_channel_status
     * with status="online" for our channel. start_stream is rejected
     * with "channel is not ready" until this flips true even though
     * the logon response has already arrived. */
    bool channel_ready;

    /* RX stream state. Zello permits multiple concurrent streams in
     * principle, but a single channel typically has one speaker at a
     * time; we accept the active stream's id+rate and decode only that
     * stream. A different stream_id arriving mid-flight logs and gets
     * dropped — fine for the listen-only bridge use case. */
    uint32_t rx_stream_id;
    int      rx_sample_rate;
    bool     rx_active;

    /* TX state. Caller streams PCM into tx_pcm_buf; full frames are
     * encoded and either sent immediately (tx_active) or buffered
     * (tx_pending — start_stream in flight). On start_stream response
     * tx_stream_id is filled and the pending queue is drained. */
    bool     tx_pending;
    bool     tx_active;
    int      tx_frame_samples;   /* size of one Opus frame in PCM samples */
    int16_t *tx_pcm_buf;         /* holds partial frame (< tx_frame_samples) */
    int      tx_pcm_n;
    uint8_t **tx_pending_q;      /* malloc'd opus payloads (without bin header) */
    size_t   *tx_pending_q_len;
    size_t    tx_pending_q_n;
    size_t    tx_pending_q_cap;
};

/* Helper used across files. */
long zello_now_ms(void);

/* ── WS → client event handlers (implemented in zello_client.c) ── */
void zello_on_ws_established(struct zello_client *c);
void zello_on_ws_error      (struct zello_client *c, const char *reason);
void zello_on_ws_closed     (struct zello_client *c);
void zello_on_ws_message    (struct zello_client *c, bool binary,
                              const uint8_t *buf, size_t len);

#endif
