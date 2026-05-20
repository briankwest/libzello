/*
 * zello_listen — minimal listen-only demo for libzello.
 *
 * Pre-alpha: connects nothing yet, just exercises the public API surface
 * so we know the headers and linkage work end-to-end. Once the WS+logon
 * task lands this prints stream activity from a real Zello channel.
 *
 * Usage:
 *   ZELLO_AUTH_TOKEN=<jwt> ZELLO_USERNAME=<u> ZELLO_PASSWORD=<p> \
 *     ZELLO_CHANNEL=<chan> ./zello_listen
 */

#include <libzello/zello_client.h>
#include <libzello/version.h>

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t g_stop = 0;
static void on_sigint(int s) { (void)s; g_stop = 1; }

static void on_connected(zello_client_t *c, const char *rt, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[connected] refresh_token=%s\n", rt ? "(received)" : "(none)");
}

static void on_disconnected(zello_client_t *c, int code, const char *reason, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[disconnected] code=%d reason=%s\n", code, reason ? reason : "");
}

static void on_channel_status(zello_client_t *c, const char *chan,
                              bool online, int users_online, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[channel] %s online=%d users=%d\n", chan, online, users_online);
}

static void on_stream_start(zello_client_t *c, uint32_t sid, const char *from,
                            int sr, int frame_ms, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[stream %u] start from=%s sr=%d frame=%dms\n",
            sid, from ? from : "?", sr, frame_ms);
}

static void on_audio(zello_client_t *c, uint32_t sid, const int16_t *pcm,
                     size_t n, int sr, void *ud)
{
    (void)c; (void)ud; (void)pcm; (void)sr;
    fprintf(stderr, "[stream %u] %zu samples\n", sid, n);
}

static void on_stream_stop(zello_client_t *c, uint32_t sid, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[stream %u] stop\n", sid);
}

static void on_error(zello_client_t *c, const char *code, void *ud)
{
    (void)c; (void)ud;
    fprintf(stderr, "[error] %s\n", code ? code : "?");
}

int main(void)
{
    signal(SIGINT, on_sigint);

    fprintf(stderr, "libzello %s\n", zello_version_string());

    zello_config_t cfg = {
        .server_url  = getenv("ZELLO_SERVER")   ? getenv("ZELLO_SERVER")   : "wss://zello.io/ws",
        .username    = getenv("ZELLO_USERNAME"),
        .password    = getenv("ZELLO_PASSWORD"),
        .channel     = getenv("ZELLO_CHANNEL"),
        .auth_token  = getenv("ZELLO_AUTH_TOKEN"),
        .listen_only = true,
    };

    if (!cfg.username || !cfg.channel) {
        fprintf(stderr, "set ZELLO_USERNAME and ZELLO_CHANNEL (and ZELLO_AUTH_TOKEN for F&F)\n");
        return 2;
    }

    zello_callbacks_t cb = {
        .on_connected      = on_connected,
        .on_disconnected   = on_disconnected,
        .on_channel_status = on_channel_status,
        .on_stream_start   = on_stream_start,
        .on_audio          = on_audio,
        .on_stream_stop    = on_stream_stop,
        .on_error          = on_error,
    };

    zello_client_t *c = zello_client_create(&cfg, &cb);
    if (!c) { fprintf(stderr, "zello_client_create failed\n"); return 1; }

    if (zello_client_start(c) != ZELLO_OK) {
        fprintf(stderr, "zello_client_start failed (expected in pre-alpha)\n");
    }

    while (!g_stop) {
        zello_client_poll(c, 100);
    }

    zello_client_stop(c);
    zello_client_destroy(c);
    return 0;
}
