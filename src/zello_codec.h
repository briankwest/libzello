#ifndef LIBZELLO_CODEC_H
#define LIBZELLO_CODEC_H

#include "libzello/zello.h"

struct zello_enc;
struct zello_dec;

struct zello_enc *zello_enc_create(int sample_rate, int frame_ms,
                                     int frames_per_packet, int bitrate);
void              zello_enc_destroy(struct zello_enc *e);

/* Encode one frame of PCM. `pcm` must hold frame_size_samples values.
 * `out` receives the Opus payload (at most max_out bytes); returns the
 * encoded length, or negative on error. */
int zello_enc_encode(struct zello_enc *e,
                      const int16_t *pcm, uint8_t *out, size_t max_out);

int zello_enc_frame_samples(const struct zello_enc *e);

/* Reset encoder state. Call this at the start of a new logical TX so
 * residual prediction state from the previous stream can't leak into
 * the first few frames of the new one — listeners see each stream as
 * a fresh decode session, so the encoder side should match. */
void zello_enc_reset(struct zello_enc *e);

struct zello_dec *zello_dec_create(int sample_rate);
void              zello_dec_destroy(struct zello_dec *d);

/* Decode one Opus packet. `pcm_out` must hold at least max_samples mono
 * int16 samples; returns the number of samples decoded, or negative. */
int zello_dec_decode(struct zello_dec *d,
                      const uint8_t *opus, size_t opus_len,
                      int16_t *pcm_out, size_t max_samples);

#endif
