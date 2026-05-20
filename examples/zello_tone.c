/*
 * zello_tone — connect to a Zello channel and transmit a short tone.
 *
 * Sends 1.5 seconds of a 440 Hz sine wave at 16 kHz, then stops the
 * stream and disconnects. Used to validate the libzello TX path
 * end-to-end against a real Zello server.
 *
 *   ZELLO_AUTH_TOKEN=<jwt> ZELLO_USERNAME=<u> ZELLO_PASSWORD=<p> \
 *     ZELLO_CHANNEL=<chan> ./zello_tone
 */

#include <libzello/zello_client.h>
#include <libzello/zello.h>

#define _USE_MATH_DEFINES
#include <math.h>
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SAMPLE_RATE   16000
#define FRAME_MS      60
#define FRAME_SAMPLES (SAMPLE_RATE * FRAME_MS / 1000) /* 960 */
#define DURATION_MS   1500
#define TOTAL_FRAMES  (DURATION_MS / FRAME_MS)
#define TONE_HZ       440.0

static volatile int g_connected = 0;
static volatile int g_done      = 0;

static void on_connected(zello_client_t *c, const char *rt, void *ud)
{
    (void)c; (void)rt; (void)ud;
    g_connected = 1;
    fprintf(stderr, "[connected]\n");
}

static void on_disconnected(zello_client_t *c, int code, const char *reason, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[disconnected] code=%d reason=%s\n", code, reason ? reason : "");
    g_done = 1;
}

static void on_channel_status(zello_client_t *c, const char *chan,
                              bool online, int users, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[channel] %s online=%d users=%d\n", chan, online, users);
}

static void on_error(zello_client_t *c, const char *code, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[error] %s\n", code ? code : "?");
}

int main(void)
{
    fprintf(stderr, "libzello %s\n", zello_version_string());

    zello_config_t cfg = {
        .server_url     = getenv("ZELLO_SERVER") ? getenv("ZELLO_SERVER") : "wss://zello.io/ws",
        .username       = getenv("ZELLO_USERNAME"),
        .password       = getenv("ZELLO_PASSWORD"),
        .channel        = getenv("ZELLO_CHANNEL"),
        .auth_token     = getenv("ZELLO_AUTH_TOKEN"),
        .listen_only    = false,
        .tx_sample_rate = SAMPLE_RATE,
        .tx_frame_ms    = FRAME_MS,
    };
    if (!cfg.username || !cfg.channel) {
        fprintf(stderr, "set ZELLO_USERNAME and ZELLO_CHANNEL\n");
        return 2;
    }

    zello_callbacks_t cb = {
        .on_connected      = on_connected,
        .on_disconnected   = on_disconnected,
        .on_channel_status = on_channel_status,
        .on_error          = on_error,
    };

    zello_client_t *c = zello_client_create(&cfg, &cb);
    if (!c) { fprintf(stderr, "create failed\n"); return 1; }
    if (zello_client_start(c) != ZELLO_OK) {
        fprintf(stderr, "start failed\n");
        zello_client_destroy(c);
        return 1;
    }

    /* Wait up to 5 s for logon. */
    int waited = 0;
    while (!g_connected && waited < 5000) {
        zello_client_poll(c, 100);
        waited += 100;
    }
    if (!g_connected) {
        fprintf(stderr, "timed out waiting for logon\n");
        zello_client_destroy(c);
        return 1;
    }

    fprintf(stderr, "[tx] start_stream\n");
    if (zello_client_start_tx(c) != ZELLO_OK) {
        fprintf(stderr, "start_tx failed\n");
        zello_client_destroy(c);
        return 1;
    }

    int16_t frame[FRAME_SAMPLES];
    double phase = 0.0;
    const double dphase = 2.0 * M_PI * TONE_HZ / SAMPLE_RATE;
    const int16_t amp = 12000;

    for (int f = 0; f < TOTAL_FRAMES; f++) {
        for (int i = 0; i < FRAME_SAMPLES; i++) {
            frame[i] = (int16_t)(sin(phase) * amp);
            phase += dphase;
            if (phase > 2.0 * M_PI) phase -= 2.0 * M_PI;
        }
        zello_client_send_pcm(c, frame, FRAME_SAMPLES);
        /* Pace at frame rate so the encoder buffer doesn't blow up;
         * also gives the WS event loop time to drain. */
        zello_client_poll(c, FRAME_MS);
    }

    fprintf(stderr, "[tx] stop_stream\n");
    zello_client_stop_tx(c);

    /* Flush any tail packets and the stop_stream JSON. */
    for (int i = 0; i < 5; i++) zello_client_poll(c, 100);

    zello_client_stop(c);
    zello_client_destroy(c);
    return 0;
}
