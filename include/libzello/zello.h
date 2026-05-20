/*
 * zello.h — Common types for libzello.
 *
 * libzello implements the Zello Channel API (WebSocket + JSON control,
 * binary Opus audio) for use with Zello Friends & Family (zello.io) and
 * Zello Work (zellowork.io). One channel per client, full duplex.
 */

#ifndef LIBZELLO_ZELLO_H
#define LIBZELLO_ZELLO_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ZELLO_OK              0
#define ZELLO_ERR            -1
#define ZELLO_ERR_AUTH       -2
#define ZELLO_ERR_TIMEOUT    -3
#define ZELLO_ERR_NETWORK    -4
#define ZELLO_ERR_STATE      -5
#define ZELLO_ERR_NOMEM      -6
#define ZELLO_ERR_PROTOCOL   -7
#define ZELLO_ERR_CODEC      -8

typedef enum {
    ZELLO_STATE_OFFLINE    = 0,
    ZELLO_STATE_CONNECTING = 1,
    ZELLO_STATE_LOGON      = 2,
    ZELLO_STATE_ONLINE     = 3,
    ZELLO_STATE_RECONNECT  = 4,
} zello_state_t;

#define ZELLO_LOG_DEBUG  0
#define ZELLO_LOG_INFO   1
#define ZELLO_LOG_WARN   2
#define ZELLO_LOG_ERROR  3

/* Audio: Opus mono. The Zello codec_header carries (sample_rate,
 * frames_per_packet, frame_size_ms). libzello defaults are below; the
 * server-side codec_header on inbound streams may differ and is exposed
 * to the caller via on_stream_start. */
#define ZELLO_DEFAULT_SAMPLE_RATE     16000
#define ZELLO_DEFAULT_FRAME_MS        60
#define ZELLO_DEFAULT_FRAMES_PER_PKT  1
/* 60 ms @ 16 kHz = 960 samples. */
#define ZELLO_DEFAULT_FRAME_SAMPLES   960

#ifdef __cplusplus
}
#endif

#endif
