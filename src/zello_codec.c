/*
 * zello_codec.c — Opus encoder/decoder wrappers.
 *
 * Skeleton: holds the OpusEncoder/OpusDecoder allocations but does not
 * yet integrate with the WS path. Real encode/decode lands with the RX
 * and TX tasks; this file already calls libopus so we know it links.
 */

#include "zello_codec.h"
#include "zello_log.h"

#include <opus/opus.h>
#include <stdlib.h>
#include <string.h>

struct zello_enc {
    OpusEncoder *enc;
    int sample_rate;
    int frame_ms;
    int frames_per_packet;
    int frame_samples;
};

struct zello_dec {
    OpusDecoder *dec;
    int sample_rate;
};

struct zello_enc *zello_enc_create(int sample_rate, int frame_ms,
                                     int frames_per_packet, int bitrate)
{
    if (sample_rate <= 0) sample_rate = ZELLO_DEFAULT_SAMPLE_RATE;
    if (frame_ms <= 0)    frame_ms = ZELLO_DEFAULT_FRAME_MS;
    if (frames_per_packet <= 0) frames_per_packet = ZELLO_DEFAULT_FRAMES_PER_PKT;
    if (bitrate <= 0)     bitrate = 24000;

    struct zello_enc *e = calloc(1, sizeof(*e));
    if (!e) return NULL;
    e->sample_rate       = sample_rate;
    e->frame_ms          = frame_ms;
    e->frames_per_packet = frames_per_packet;
    e->frame_samples     = sample_rate * frame_ms / 1000;

    int err = 0;
    e->enc = opus_encoder_create(sample_rate, 1, OPUS_APPLICATION_VOIP, &err);
    if (!e->enc || err != OPUS_OK) {
        ZLOG_E("opus_encoder_create failed: %s", opus_strerror(err));
        free(e);
        return NULL;
    }
    opus_encoder_ctl(e->enc, OPUS_SET_BITRATE(bitrate));
    opus_encoder_ctl(e->enc, OPUS_SET_INBAND_FEC(1));
    opus_encoder_ctl(e->enc, OPUS_SET_PACKET_LOSS_PERC(10));
    return e;
}

void zello_enc_destroy(struct zello_enc *e)
{
    if (!e) return;
    if (e->enc) opus_encoder_destroy(e->enc);
    free(e);
}

int zello_enc_frame_samples(const struct zello_enc *e)
{
    return e ? e->frame_samples : 0;
}

int zello_enc_encode(struct zello_enc *e,
                      const int16_t *pcm, uint8_t *out, size_t max_out)
{
    if (!e || !pcm || !out) return ZELLO_ERR;
    int n = opus_encode(e->enc, pcm, e->frame_samples, out, (opus_int32)max_out);
    if (n < 0) {
        ZLOG_W("opus_encode failed: %s", opus_strerror(n));
        return ZELLO_ERR_CODEC;
    }
    return n;
}

struct zello_dec *zello_dec_create(int sample_rate)
{
    if (sample_rate <= 0) sample_rate = ZELLO_DEFAULT_SAMPLE_RATE;
    struct zello_dec *d = calloc(1, sizeof(*d));
    if (!d) return NULL;
    d->sample_rate = sample_rate;
    int err = 0;
    d->dec = opus_decoder_create(sample_rate, 1, &err);
    if (!d->dec || err != OPUS_OK) {
        ZLOG_E("opus_decoder_create failed: %s", opus_strerror(err));
        free(d);
        return NULL;
    }
    return d;
}

void zello_dec_destroy(struct zello_dec *d)
{
    if (!d) return;
    if (d->dec) opus_decoder_destroy(d->dec);
    free(d);
}

int zello_dec_decode(struct zello_dec *d,
                      const uint8_t *opus, size_t opus_len,
                      int16_t *pcm_out, size_t max_samples)
{
    if (!d || !pcm_out) return ZELLO_ERR;
    int n = opus_decode(d->dec, opus, (opus_int32)opus_len,
                        pcm_out, (int)max_samples, 0);
    if (n < 0) {
        ZLOG_W("opus_decode failed: %s", opus_strerror(n));
        return ZELLO_ERR_CODEC;
    }
    return n;
}
