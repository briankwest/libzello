/*
 * zello_proto.h — Zello Channel API wire-protocol constants.
 *
 * Reference: https://github.com/zelloptt/zello-channel-api/blob/main/API.md
 *
 * Control plane: WebSocket TEXT frames carrying JSON. Each request has a
 * unique `seq` and a `command`; responses echo `seq` and report success/error.
 *
 * Audio plane: WebSocket BINARY frames in the layout:
 *
 *   uint8_t  packet_type   (ZELLO_PKT_AUDIO = 0x01, ZELLO_PKT_IMAGE = 0x02)
 *   uint32_t stream_id     (big-endian)
 *   uint32_t packet_id     (big-endian; 0 for outbound)
 *   uint8_t  payload[]     (Opus bytes for audio packets)
 *
 * The codec_header sent on start_stream / received on on_stream_start is a
 * base64-encoded 4-byte struct:
 *
 *   uint16_t sample_rate_hz   (little-endian)
 *   uint8_t  frames_per_packet
 *   uint8_t  frame_size_ms
 */

#ifndef LIBZELLO_ZELLO_PROTO_H
#define LIBZELLO_ZELLO_PROTO_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ZELLO_PKT_AUDIO  0x01
#define ZELLO_PKT_IMAGE  0x02

#define ZELLO_BIN_HEADER_LEN  9   /* type(1) + stream_id(4) + packet_id(4) */

#define ZELLO_DEFAULT_SERVER_FF    "wss://zello.io/ws"
#define ZELLO_PING_INTERVAL_S      30

/* Encode a codec_header struct into the 4 bytes that get base64'd
 * for the start_stream message. */
static inline void zello_codec_header_pack(uint8_t out[4],
                                            uint16_t sample_rate,
                                            uint8_t frames_per_packet,
                                            uint8_t frame_size_ms)
{
    out[0] = (uint8_t)(sample_rate & 0xFF);
    out[1] = (uint8_t)((sample_rate >> 8) & 0xFF);
    out[2] = frames_per_packet;
    out[3] = frame_size_ms;
}

static inline int zello_codec_header_unpack(const uint8_t in[4],
                                             uint16_t *sample_rate,
                                             uint8_t *frames_per_packet,
                                             uint8_t *frame_size_ms)
{
    if (!in || !sample_rate || !frames_per_packet || !frame_size_ms) return -1;
    *sample_rate       = (uint16_t)in[0] | ((uint16_t)in[1] << 8);
    *frames_per_packet = in[2];
    *frame_size_ms     = in[3];
    return 0;
}

#ifdef __cplusplus
}
#endif

#endif
