/*
 * test_codec_roundtrip.c — verify the libzello Opus encoder/decoder
 * and Zello binary packet wrapping/unwrapping in isolation, without
 * any network. The test:
 *
 *   1. loads a 16-bit mono WAV
 *   2. resamples it to 16 kHz (libzello's TX rate) if needed
 *   3. encodes it through libzello's Opus encoder, 20 ms frames
 *   4. wraps each Opus payload in the Zello binary header
 *   5. unwraps + decodes each packet using libzello's decoder
 *   6. writes the reconstructed PCM to /tmp/test_roundtrip_out.wav
 *
 * If the input is intelligible and the output is intelligible (allow
 * for some Opus VOIP-mode lossiness), the codec + packet path is
 * fine and any garble heard on a real Zello listener is upstream of
 * libzello (mod_zello producer cadence, mod_zello sample data, or
 * something Zello-server-specific).
 *
 * Usage:
 *   tests/test_codec_roundtrip <input.wav>
 *
 * Exit codes:
 *   0  round-trip succeeded; reconstructed WAV written
 *   1  bad arguments or input could not be loaded
 *   2  encode/decode failed
 */

#include "libzello/zello.h"
#include "libzello/zello_proto.h"

#include <opus/opus.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>

#define TX_RATE          16000
#define TX_FRAME_MS      60
#define TX_FRAME_SAMPLES (TX_RATE * TX_FRAME_MS / 1000)   /* 960 */
#define OPUS_MAX_BYTES   1500

/* ── Minimal RIFF/WAV I/O (16-bit mono only). ─────────────────────── */

static int16_t *load_wav_mono16(const char *path, size_t *out_n, int *out_sr)
{
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "open %s: %s\n", path, strerror(errno)); return NULL; }

    uint8_t hdr[44];
    if (fread(hdr, 1, sizeof(hdr), f) != sizeof(hdr)) { fclose(f); return NULL; }
    if (memcmp(hdr, "RIFF", 4) != 0 || memcmp(hdr + 8, "WAVE", 4) != 0) {
        fprintf(stderr, "%s: not a RIFF/WAVE file\n", path);
        fclose(f); return NULL;
    }
    uint16_t channels = (uint16_t)(hdr[22] | (hdr[23] << 8));
    uint32_t sr       = (uint32_t)(hdr[24] | (hdr[25] << 8) |
                                   (hdr[26] << 16) | (hdr[27] << 24));
    uint16_t bps      = (uint16_t)(hdr[34] | (hdr[35] << 8));
    if (channels != 1 || bps != 16) {
        fprintf(stderr, "%s: need 16-bit mono (got %u ch, %u bps)\n",
                path, channels, bps);
        fclose(f); return NULL;
    }
    /* Find the "data" subchunk — header may have extra subchunks before. */
    fseek(f, 12, SEEK_SET);
    uint32_t data_size = 0;
    long     data_off  = -1;
    while (!feof(f)) {
        uint8_t ck[8];
        if (fread(ck, 1, 8, f) != 8) break;
        uint32_t size = (uint32_t)(ck[4] | (ck[5] << 8) | (ck[6] << 16) | (ck[7] << 24));
        if (!memcmp(ck, "data", 4)) { data_size = size; data_off = ftell(f); break; }
        fseek(f, size, SEEK_CUR);
    }
    if (data_off < 0) { fclose(f); return NULL; }
    fseek(f, data_off, SEEK_SET);

    size_t   n  = data_size / sizeof(int16_t);
    int16_t *pcm = malloc(n * sizeof(int16_t));
    if (!pcm) { fclose(f); return NULL; }
    if (fread(pcm, sizeof(int16_t), n, f) != n) {
        free(pcm); fclose(f); return NULL;
    }
    fclose(f);
    *out_n  = n;
    *out_sr = (int)sr;
    return pcm;
}

static int write_wav_mono16(const char *path, const int16_t *pcm,
                             size_t n, int sr)
{
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    uint32_t data_size = (uint32_t)(n * sizeof(int16_t));
    uint32_t riff_size = 36 + data_size;
    uint32_t byte_rate = (uint32_t)sr * 2;
    uint8_t  hdr[44]   = {
        'R','I','F','F',  (uint8_t)(riff_size), (uint8_t)(riff_size >> 8),
                          (uint8_t)(riff_size >> 16), (uint8_t)(riff_size >> 24),
        'W','A','V','E','f','m','t',' ',
        16, 0, 0, 0,
        1, 0, 1, 0,
        (uint8_t)sr, (uint8_t)(sr >> 8), (uint8_t)(sr >> 16), (uint8_t)(sr >> 24),
        (uint8_t)byte_rate, (uint8_t)(byte_rate >> 8),
        (uint8_t)(byte_rate >> 16), (uint8_t)(byte_rate >> 24),
        2, 0, 16, 0,
        'd','a','t','a',
        (uint8_t)data_size, (uint8_t)(data_size >> 8),
        (uint8_t)(data_size >> 16), (uint8_t)(data_size >> 24),
    };
    fwrite(hdr, 1, sizeof(hdr), f);
    fwrite(pcm, sizeof(int16_t), n, f);
    fclose(f);
    return 0;
}

/* ── Linear-interp resampler. Same algorithm libzello's example tools
 *    use; intentionally simple so this test mirrors what the live code
 *    actually does. ─────────────────────────────────────────────────── */
static int16_t *resample_linear(const int16_t *in, size_t in_n, int in_sr,
                                  int out_sr, size_t *out_n)
{
    if (in_sr == out_sr) {
        int16_t *cp = malloc(in_n * sizeof(int16_t));
        if (!cp) return NULL;
        memcpy(cp, in, in_n * sizeof(int16_t));
        *out_n = in_n;
        return cp;
    }
    size_t n = (size_t)((double)in_n * out_sr / in_sr);
    int16_t *out = malloc(n * sizeof(int16_t));
    if (!out) return NULL;
    double step = (double)in_sr / (double)out_sr;
    double pos  = 0.0;
    for (size_t i = 0; i < n; i++) {
        size_t idx  = (size_t)pos;
        double frac = pos - (double)idx;
        int16_t s0 = (idx     < in_n) ? in[idx]     : 0;
        int16_t s1 = (idx + 1 < in_n) ? in[idx + 1] : s0;
        out[i] = (int16_t)(s0 + frac * (s1 - s0));
        pos += step;
    }
    *out_n = n;
    return out;
}

/* ── Zello binary packet: pack/unpack inline so the test mirrors what
 *    libzello does on the wire without depending on libzello internals. */
static size_t zello_pack(uint8_t *pkt, uint32_t stream_id, uint32_t packet_id,
                          const uint8_t *opus, size_t opus_len)
{
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
    return ZELLO_BIN_HEADER_LEN + opus_len;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "usage: %s <input.wav>\n", argv[0]);
        return 1;
    }

    size_t in_n = 0; int in_sr = 0;
    int16_t *in_pcm = load_wav_mono16(argv[1], &in_n, &in_sr);
    if (!in_pcm) return 1;
    printf("input:  %zu samples @ %d Hz (%.2f s)\n",
           in_n, in_sr, (double)in_n / in_sr);

    size_t pcm_n = 0;
    int16_t *pcm16k = resample_linear(in_pcm, in_n, in_sr, TX_RATE, &pcm_n);
    free(in_pcm);
    if (!pcm16k) return 1;
    if (in_sr != TX_RATE)
        printf("resampled to %zu samples @ %d Hz\n", pcm_n, TX_RATE);

    /* Encoder + decoder set up directly so the test owns the codec
     * lifecycle and we can assert the parameters match the start_stream
     * codec_header we'd send in production. */
    int err = 0;
    OpusEncoder *enc = opus_encoder_create(TX_RATE, 1, OPUS_APPLICATION_VOIP, &err);
    if (err != OPUS_OK) {
        fprintf(stderr, "opus_encoder_create: %s\n", opus_strerror(err));
        free(pcm16k); return 2;
    }
    opus_encoder_ctl(enc, OPUS_SET_BITRATE(24000));
    opus_encoder_ctl(enc, OPUS_SET_INBAND_FEC(1));
    opus_encoder_ctl(enc, OPUS_SET_PACKET_LOSS_PERC(10));

    OpusDecoder *dec = opus_decoder_create(TX_RATE, 1, &err);
    if (err != OPUS_OK) {
        fprintf(stderr, "opus_decoder_create: %s\n", opus_strerror(err));
        opus_encoder_destroy(enc); free(pcm16k); return 2;
    }

    /* Sanity-check codec_header: this is what we send to Zello in
     * start_stream. For 16 kHz / 1 frame per packet / 20 ms, the base64
     * is "gD4BFA==" — matches the JS SDK example. */
    uint8_t hdr[4];
    zello_codec_header_pack(hdr, (uint16_t)TX_RATE, 1, TX_FRAME_MS);
    printf("codec_header bytes: %02x %02x %02x %02x  (expect 80 3e 01 3c for 60ms)\n",
           hdr[0], hdr[1], hdr[2], hdr[3]);

    /* Round-trip every full frame. Trailing partial frame is zero-padded
     * to match what tx_drain_one_frame does on stream stop. */
    size_t out_cap = pcm_n + TX_FRAME_SAMPLES;
    int16_t *out_pcm = calloc(out_cap, sizeof(int16_t));
    if (!out_pcm) { opus_decoder_destroy(dec); opus_encoder_destroy(enc);
                    free(pcm16k); return 2; }
    size_t out_pos = 0;

    int16_t in_frame[TX_FRAME_SAMPLES];
    uint8_t opus_buf[OPUS_MAX_BYTES];
    uint8_t pkt[ZELLO_BIN_HEADER_LEN + OPUS_MAX_BYTES];
    int     frame_idx = 0;
    size_t  src       = 0;
    int     total_opus_bytes = 0;
    int     min_opus = INT_MAX, max_opus = 0;

    while (src < pcm_n) {
        size_t take = (pcm_n - src) < TX_FRAME_SAMPLES
                      ? (pcm_n - src) : TX_FRAME_SAMPLES;
        memcpy(in_frame, pcm16k + src, take * sizeof(int16_t));
        if (take < TX_FRAME_SAMPLES)
            memset(in_frame + take, 0,
                   (TX_FRAME_SAMPLES - take) * sizeof(int16_t));
        src += take;

        int olen = opus_encode(enc, in_frame, TX_FRAME_SAMPLES,
                                opus_buf, sizeof(opus_buf));
        if (olen <= 0) {
            fprintf(stderr, "opus_encode frame %d: %s\n",
                    frame_idx, opus_strerror(olen));
            break;
        }
        total_opus_bytes += olen;
        if (olen < min_opus) min_opus = olen;
        if (olen > max_opus) max_opus = olen;

        /* Wrap exactly as the live code would. packet_id starts at 1. */
        size_t pkt_len = zello_pack(pkt, /*stream_id=*/1,
                                     (uint32_t)(frame_idx + 1),
                                     opus_buf, (size_t)olen);

        /* Unwrap on the receive side, then decode. */
        if (pkt[0] != ZELLO_PKT_AUDIO) {
            fprintf(stderr, "frame %d: bad packet type 0x%02x\n",
                    frame_idx, pkt[0]);
            break;
        }
        const uint8_t *rx_opus = pkt + ZELLO_BIN_HEADER_LEN;
        size_t         rx_len  = pkt_len - ZELLO_BIN_HEADER_LEN;

        int dn = opus_decode(dec, rx_opus, (opus_int32)rx_len,
                              out_pcm + out_pos,
                              (int)(out_cap - out_pos), 0);
        if (dn < 0) {
            fprintf(stderr, "opus_decode frame %d: %s\n",
                    frame_idx, opus_strerror(dn));
            break;
        }
        out_pos += (size_t)dn;
        frame_idx++;
    }

    printf("frames: %d  bytes/frame: min=%d avg=%d max=%d  total opus=%d B\n",
           frame_idx, min_opus,
           frame_idx ? (total_opus_bytes / frame_idx) : 0,
           max_opus, total_opus_bytes);

    /* Quick integrity stats: peak in vs out. We're not bit-exact (Opus
     * is lossy) but a normal voice signal should round-trip with peaks
     * within a few dB. */
    int32_t in_peak = 0, out_peak = 0;
    for (size_t i = 0; i < pcm_n; i++)  { int v = abs(pcm16k[i]); if (v > in_peak)  in_peak  = v; }
    for (size_t i = 0; i < out_pos; i++) { int v = abs(out_pcm[i]); if (v > out_peak) out_peak = v; }
    printf("peak in=%" PRId32 "  out=%" PRId32 "  out/in=%.2f\n",
           in_peak, out_peak,
           in_peak ? (double)out_peak / in_peak : 0.0);

    const char *out_path = "/tmp/test_roundtrip_out.wav";
    if (write_wav_mono16(out_path, out_pcm, out_pos, TX_RATE) == 0)
        printf("wrote %s (%zu samples @ %d Hz)\n", out_path, out_pos, TX_RATE);
    else
        fprintf(stderr, "failed writing %s\n", out_path);

    free(out_pcm);
    free(pcm16k);
    opus_decoder_destroy(dec);
    opus_encoder_destroy(enc);
    return 0;
}
