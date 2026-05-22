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

#include <pthread.h>
#include <stdatomic.h>
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

    /* Coarse-grained recursive lock around all public-API calls and
     * all WS-callback dispatch. Recursive so that user callbacks
     * (fired from inside zello_client_poll while we hold the lock)
     * may freely call back into libzello (e.g., start_tx from
     * on_connected) without self-deadlock. */
    pthread_mutex_t mtx;

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

    /* TX path — lock-free producer/consumer.
     *
     * Producer (any thread) pushes PCM samples into tx_ring via
     * zello_client_send_pcm(). Consumer (the service thread inside
     * zello_client_poll) drains the ring at real-time cadence, encodes
     * one Opus frame per slot, and queues the binary packet to lws.
     *
     * start_tx and stop_tx are non-blocking — they set atomic flags
     * (tx_req_start / tx_req_stop) that the service thread acts on at
     * its next pass. Caller latency is decoupled from lws_service
     * timing, and the service thread paces audio at frame_ms intervals
     * so listeners hear smooth real-time audio regardless of how fast
     * the caller pushed samples in. */
    bool     tx_pending;
    bool     tx_active;
    bool     tx_stop_after_drain;  /* set on stop request while still draining */
    int      tx_frame_samples;
    long     tx_next_encode_ms;    /* wall-clock target for next encode */
    int      tx_frames_sent;       /* diagnostic: frames drained in current stream */
    int16_t *tx_frame_buf;         /* scratch buffer for one frame */

    /* PCM ring — sized in zello_client_create for ~2 s of audio. */
    int16_t      *tx_ring;
    size_t        tx_ring_cap;     /* number of int16 slots */
    atomic_size_t tx_ring_w;       /* producer index (monotonic) */
    atomic_size_t tx_ring_r;       /* consumer index (monotonic) */

    /* Request flags set by start_tx/stop_tx; cleared by service thread. */
    atomic_int    tx_req_start;
    atomic_int    tx_req_stop;

    /* Producer gate. send_pcm only writes to the ring while this is 1.
     * Lifecycle:
     *   start_tx:    set to 1 LAST, after the ring indices are reset.
     *                Any producer thread observing 1 also observes the
     *                fresh ring state.
     *   stop_tx:     set to 0 FIRST. Any producer push after this point
     *                is dropped, so the service thread can drain what's
     *                in the ring without racing further writes. */
    atomic_int    tx_streaming;
};

/* Helper used across files. */
long zello_now_ms(void);

/* Locking helpers — wrappers so we can switch to a different model
 * later (e.g., a lock-free SPSC ring for the TX hot path) without
 * touching every call site. */
static inline void zcli_lock  (struct zello_client *c) { pthread_mutex_lock  (&c->mtx); }
static inline void zcli_unlock(struct zello_client *c) { pthread_mutex_unlock(&c->mtx); }

/* ── WS → client event handlers (implemented in zello_client.c) ── */
void zello_on_ws_established(struct zello_client *c);
void zello_on_ws_error      (struct zello_client *c, const char *reason);
void zello_on_ws_closed     (struct zello_client *c);
void zello_on_ws_message    (struct zello_client *c, bool binary,
                              const uint8_t *buf, size_t len);

#endif
