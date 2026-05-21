/*
 * zello_cli — interactive text-mode Zello channel client for libzello.
 *
 * Connects to a Zello channel, prints inbound stream activity, and
 * accepts text commands on stdin to exercise TX, text, and status.
 * No audio device I/O — TX uses synthesized tones or pre-recorded
 * WAV files so the tool stays useful on headless machines.
 *
 * Configuration (env vars):
 *   ZELLO_SERVER       wss://… (default wss://zello.io/ws)
 *   ZELLO_USERNAME     consumer Zello account name
 *   ZELLO_PASSWORD     consumer Zello account password
 *   ZELLO_CHANNEL      channel to join on logon
 *   ZELLO_AUTH_TOKEN   developer JWT (required for F&F)
 *   ZELLO_WS_DEBUG=1   bump libwebsockets to NOTICE-level trace
 *
 * Commands (one per line, then Enter):
 *   help                        list commands
 *   status                      connection state + channel
 *   tone [hz] [ms]              transmit a sine wave (defaults 440 Hz, 1500 ms)
 *   wav <path>                  transmit a 16-bit mono WAV (any sample rate)
 *   say <text>                  send a channel text message
 *   quit | exit                 clean disconnect + exit
 *   Ctrl-D / Ctrl-C             same as quit
 */

#define _POSIX_C_SOURCE 200809L

#include <libzello/zello_client.h>
#include <libzello/zello.h>

#include "linenoise.h"

#include <math.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

/* Match libzello defaults so we don't force a sample-rate change on the
 * server side. 16 kHz mono, 60 ms frames = 960 samples per frame. */
#define TX_RATE          16000
#define TX_FRAME_MS      60
#define TX_FRAME_SAMPLES (TX_RATE * TX_FRAME_MS / 1000)

static volatile sig_atomic_t g_quit       = 0;
static bool                  g_connected  = false;
static char                  g_channel[64];

/* Thread layout
 *
 *   main thread   — runs the linenoise non-blocking editor + drains the
 *                   log queue, performing hide/show around each log line
 *                   so the prompt always stays at the bottom of the
 *                   terminal. Owns all linenoise state.
 *   zello thread  — calls zello_client_poll(c, 0) in a tight loop. Owns
 *                   lws_service. WS callbacks fire on this thread, and
 *                   they enqueue display messages instead of printing
 *                   directly — so the main thread can hide/show around
 *                   them safely.
 *
 * libzello's internal recursive mutex serializes lws_service against
 * any libzello calls main makes for commands; contention is brief. */

static pthread_t       g_zello_thread;
static int             g_zello_thread_started = 0;
static zello_client_t *g_cli = NULL;

static struct linenoiseState g_ls;
static int                   g_editing = 0;

/* Command queue — commands run on a dedicated worker thread so the
 * prompt returns immediately even when the command takes seconds
 * (cmd_tone, cmd_wav). Output from commands goes through cli_log so
 * it lands in the log queue, gets drained by the main thread, and
 * stays cleanly above the prompt. */
struct cmd_node {
    char            *line;
    struct cmd_node *next;
};
static pthread_t        g_cmd_thread;
static int              g_cmd_thread_started = 0;
static pthread_mutex_t  g_cmdq_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t   g_cmdq_cv  = PTHREAD_COND_INITIALIZER;
static struct cmd_node *g_cmdq_head = NULL;
static struct cmd_node **g_cmdq_tail = NULL;

/* Log queue — populated by libzello callbacks on the zello thread,
 * drained by the main thread which holds linenoise state. */
struct log_node {
    char            *msg;
    struct log_node *next;
};
static pthread_mutex_t g_logq_mtx  = PTHREAD_MUTEX_INITIALIZER;
static struct log_node *g_logq_head = NULL;
static struct log_node **g_logq_tail = NULL;

static void on_sigint(int s) { (void)s; g_quit = 1; }

static long now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Enqueue one log line. Safe to call from any thread — the main thread
 * drains the queue between linenoise events and emits each line with
 * proper hide/show framing. ZELLO_CLI_QUIET=1 drops all messages. */
static int g_log_silent = 0;

static void cli_log(const char *fmt, ...)
{
    if (g_log_silent) return;
    char    buf[2048];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf) - 2, fmt, ap);
    va_end(ap);
    if (n < 0) return;
    /* Ensure single trailing newline. */
    while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r')) n--;
    buf[n++] = '\n';
    buf[n]   = 0;

    struct log_node *node = malloc(sizeof(*node));
    if (!node) return;
    node->msg  = strdup(buf);
    node->next = NULL;
    if (!node->msg) { free(node); return; }

    pthread_mutex_lock(&g_logq_mtx);
    if (!g_logq_tail) g_logq_tail = &g_logq_head;
    *g_logq_tail = node;
    g_logq_tail  = &node->next;
    pthread_mutex_unlock(&g_logq_mtx);
}

/* Pop the entire queue under the lock; return a singly-linked list the
 * caller can iterate and free at leisure (without holding the lock). */
static struct log_node *log_drain_all(void)
{
    pthread_mutex_lock(&g_logq_mtx);
    struct log_node *list = g_logq_head;
    g_logq_head = NULL;
    g_logq_tail = &g_logq_head;
    pthread_mutex_unlock(&g_logq_mtx);
    return list;
}

/* Hide the linenoise prompt, emit all queued logs, then redraw. Called
 * only from the main thread, which is the sole user of linenoise state. */
static void log_flush_to_terminal(void)
{
    struct log_node *list = log_drain_all();
    if (!list) return;
    if (g_editing) linenoiseHide(&g_ls);
    for (struct log_node *n = list; n; ) {
        struct log_node *next = n->next;
        fputs(n->msg, stderr);
        free(n->msg);
        free(n);
        n = next;
    }
    if (g_editing) linenoiseShow(&g_ls);
}

static void zello_log_handler(int level, const char *msg, void *ud)
{
    (void)ud;
    static const char *tag[] = { "D", "I", "W", "E" };
    const char *t = (level >= 0 && level <= 3) ? tag[level] : "?";
    cli_log("[zello %s] %s", t, msg);
}


/* ── libzello callbacks ──────────────────────────────────────────── */

static void on_connected(zello_client_t *c, const char *rt, void *ud)
{
    (void)c; (void)ud;
    g_connected = true;
    cli_log("[connected] refresh_token=%s",
            rt && *rt ? "captured" : "(none)");
}

static void on_disconnected(zello_client_t *c, int code, const char *reason, void *ud)
{
    (void)c; (void)ud;
    g_connected = false;
    cli_log("[disconnected] code=%d reason=%s", code, reason ? reason : "");
}

static void on_channel_status(zello_client_t *c, const char *chan,
                              bool online, int users, void *ud)
{
    (void)c; (void)ud;
    cli_log("[channel] %s %s (%d users)",
            chan ? chan : "?", online ? "online" : "offline", users);
}

static void on_stream_start(zello_client_t *c, uint32_t sid, const char *from,
                            int sr, int frame_ms, void *ud)
{
    (void)c; (void)ud;
    cli_log("[rx %u] start from=%s sr=%d frame=%dms",
            sid, from ? from : "?", sr, frame_ms);
}

static void on_audio(zello_client_t *c, uint32_t sid, const int16_t *pcm,
                     size_t n, int sr, void *ud)
{
    (void)c; (void)pcm; (void)ud;
    static uint32_t last_sid     = 0;
    static size_t   total_n      = 0;
    static int      pkt_in_burst = 0;
    if (sid != last_sid) { last_sid = sid; total_n = 0; pkt_in_burst = 0; }
    total_n += n;
    pkt_in_burst++;
    if ((pkt_in_burst % 5) == 0) {
        cli_log("[rx %u] %zu samples @ %d Hz (%.1f s)",
                sid, total_n, sr, (double)total_n / sr);
    }
}

static void on_stream_stop(zello_client_t *c, uint32_t sid, void *ud)
{
    (void)c; (void)ud;
    cli_log("[rx %u] stop", sid);
}

static void on_text_message(zello_client_t *c, const char *from,
                            const char *text, void *ud)
{
    (void)c; (void)ud;
    cli_log("[text] from=%s: %s", from ? from : "?", text ? text : "");
}

static void on_error(zello_client_t *c, const char *code, void *ud)
{
    (void)c; (void)ud;
    cli_log("[error] %s", code ? code : "?");
}

/* ── Command handlers ───────────────────────────────────────────── */

static const char *state_name(zello_state_t s)
{
    switch (s) {
    case ZELLO_STATE_OFFLINE:    return "offline";
    case ZELLO_STATE_CONNECTING: return "connecting";
    case ZELLO_STATE_LOGON:      return "logon";
    case ZELLO_STATE_ONLINE:     return "online";
    case ZELLO_STATE_RECONNECT:  return "reconnect";
    }
    return "?";
}

static void cmd_help(void)
{
    cli_log("Commands:");
    cli_log("  help                       this list");
    cli_log("  status                     connection state and channel");
    cli_log("  tone [hz] [ms]             transmit a sine wave (default 440 Hz, 1500 ms)");
    cli_log("  wav <path>                 transmit a 16-bit mono WAV (any sample rate)");
    cli_log("  say <text>                 send a channel text message");
    cli_log("  quit | exit                clean disconnect and exit");
}

static void cmd_status(zello_client_t *c)
{
    cli_log("state=%s channel=%s connected=%s",
            state_name(zello_client_state(c)), g_channel,
            g_connected ? "yes" : "no");
}

static int cmd_tone(zello_client_t *c, int hz, int ms)
{
    int rc = zello_client_start_tx(c);
    if (rc != ZELLO_OK) {
        cli_log("start_tx failed: rc=%d", rc);
        return rc;
    }

    int     total_frames = ms / TX_FRAME_MS;
    int16_t frame[TX_FRAME_SAMPLES];
    double  phase        = 0.0;
    double  dphase       = 2.0 * M_PI * hz / TX_RATE;
    int16_t amp          = 12000;

    cli_log("tx: %d Hz tone for %d ms (%d x 60ms frames)", hz, ms, total_frames);

    /* Generate + queue all frames as fast as libzello accepts them;
     * see cmd_wav for why pacing here is counterproductive. */
    for (int f = 0; f < total_frames && !g_quit; f++) {
        for (int i = 0; i < TX_FRAME_SAMPLES; i++) {
            frame[i] = (int16_t)(sin(phase) * amp);
            phase += dphase;
            if (phase > 2.0 * M_PI) phase -= 2.0 * M_PI;
        }
        if (zello_client_send_pcm(c, frame, TX_FRAME_SAMPLES) != ZELLO_OK) break;
    }

    /* Wait wall-clock playback duration so the server can relay every
     * frame before we tell it to stop. */
    usleep((useconds_t)(ms + 500) * 1000);

    zello_client_stop_tx(c);
    usleep(300000);
    cli_log("tx done");
    return 0;
}

/* Minimal WAV reader: 16-bit PCM mono only. Scans chunks to locate `data`. */
static int16_t *read_wav(const char *path, size_t *out_n, int *out_sr)
{
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); return NULL; }

    uint8_t hdr[12];
    if (fread(hdr, 1, 12, f) != 12 ||
        memcmp(hdr, "RIFF", 4) || memcmp(hdr + 8, "WAVE", 4)) {
        cli_log("%s: not a RIFF/WAVE file", path);
        fclose(f);
        return NULL;
    }

    int    sr = 0, channels = 0, bps = 0;
    size_t data_bytes = 0;
    long   data_offset = -1;

    for (;;) {
        uint8_t ck[8];
        if (fread(ck, 1, 8, f) != 8) break;
        uint32_t ck_size = ck[4] | (ck[5] << 8) | (ck[6] << 16) | (ck[7] << 24);
        if (!memcmp(ck, "fmt ", 4)) {
            uint8_t fmt[16];
            size_t  rd = fread(fmt, 1, ck_size < 16 ? ck_size : 16, f);
            (void)rd;
            channels = fmt[2] | (fmt[3] << 8);
            sr       = fmt[4] | (fmt[5] << 8) | (fmt[6] << 16) | (fmt[7] << 24);
            bps      = fmt[14] | (fmt[15] << 8);
            if (ck_size > 16) fseek(f, ck_size - 16, SEEK_CUR);
        } else if (!memcmp(ck, "data", 4)) {
            data_offset = ftell(f);
            data_bytes  = ck_size;
            break;
        } else {
            fseek(f, ck_size, SEEK_CUR);
        }
    }

    if (data_offset < 0 || channels != 1 || bps != 16) {
        cli_log("%s: need 16-bit mono PCM (got ch=%d bps=%d data_off=%ld)",
                path, channels, bps, data_offset);
        fclose(f);
        return NULL;
    }

    fseek(f, data_offset, SEEK_SET);
    size_t   n_samples = data_bytes / 2;
    int16_t *pcm       = malloc(n_samples * sizeof(int16_t));
    if (!pcm) { fclose(f); return NULL; }
    if (fread(pcm, 2, n_samples, f) != n_samples) {
        free(pcm);
        fclose(f);
        return NULL;
    }
    fclose(f);

    *out_n  = n_samples;
    *out_sr = sr;
    return pcm;
}

/* Linear interpolation resampler — adequate for tone-quality tests. */
static int16_t *resample_linear(const int16_t *in, size_t in_n, int in_sr,
                                 int out_sr, size_t *out_n)
{
    if (in_sr == out_sr) {
        int16_t *copy = malloc(in_n * sizeof(int16_t));
        if (!copy) return NULL;
        memcpy(copy, in, in_n * sizeof(int16_t));
        *out_n = in_n;
        return copy;
    }
    size_t   out_max = (size_t)((double)in_n * out_sr / in_sr) + 8;
    int16_t *out     = malloc(out_max * sizeof(int16_t));
    if (!out) return NULL;
    double step = (double)in_sr / out_sr;
    size_t i    = 0;
    for (double x = 0.0; i < out_max && x < in_n - 1; x += step, i++) {
        size_t idx = (size_t)x;
        double f   = x - idx;
        out[i]     = (int16_t)((1.0 - f) * in[idx] + f * in[idx + 1]);
    }
    *out_n = i;
    return out;
}

static int cmd_wav(zello_client_t *c, const char *path)
{
    long t_start = now_ms();
    size_t   pcm_n = 0;
    int      sr    = 0;
    int16_t *pcm   = read_wav(path, &pcm_n, &sr);
    if (!pcm) return -1;
    long t_load = now_ms();
    cli_log("wav: loaded %zu samples @ %d Hz (%.2f s) [load %ldms]",
            pcm_n, sr, (double)pcm_n / sr, t_load - t_start);

    size_t   out_n = 0;
    int16_t *out   = resample_linear(pcm, pcm_n, sr, TX_RATE, &out_n);
    free(pcm);
    if (!out) return -1;
    long t_resample = now_ms();
    if (sr != TX_RATE)
        cli_log("wav: resampled to %zu samples @ %d Hz [resample %ldms]",
                out_n, TX_RATE, t_resample - t_load);

    long t_st0 = now_ms();
    if (zello_client_start_tx(c) != ZELLO_OK) {
        cli_log("start_tx failed");
        free(out);
        return -1;
    }
    long t_st1 = now_ms();
    cli_log("wav: start_tx returned [%ldms]", t_st1 - t_st0);

    size_t sent = 0;
    long t_send0 = now_ms();
    int frame_idx = 0;
    while (sent < out_n && !g_quit) {
        size_t chunk = (out_n - sent) > TX_FRAME_SAMPLES
                       ? TX_FRAME_SAMPLES : (out_n - sent);
        long ts = now_ms();
        if (zello_client_send_pcm(c, out + sent, chunk) != ZELLO_OK) break;
        long elapsed = now_ms() - ts;
        if (elapsed > 50)
            cli_log("wav: send_pcm[%d] = %ldms (chunk=%zu)", frame_idx, elapsed, chunk);
        sent += chunk;
        frame_idx++;
    }
    long t_send1 = now_ms();
    cli_log("wav: send loop total %ldms (%zu frames)", t_send1 - t_send0, (size_t)frame_idx);

    long playback_ms = (long)((double)out_n / TX_RATE * 1000.0);
    usleep((useconds_t)(playback_ms + 500) * 1000);
    long t_wait = now_ms();

    long t_stop0 = now_ms();
    zello_client_stop_tx(c);
    long t_stop1 = now_ms();
    usleep(300000);
    long t_end = now_ms();

    free(out);
    cli_log("wav: TOTAL %ldms (load=%ld resample=%ld start_tx=%ld send=%ld wait=%ld stop_tx=%ld tail=%ld)",
            t_end - t_start,
            t_load - t_start,
            t_resample - t_load,
            t_st1 - t_st0,
            t_send1 - t_send0,
            t_wait - t_send1,
            t_stop1 - t_stop0,
            t_end - t_stop1);
    return 0;
}

static int cmd_say(zello_client_t *c, const char *text)
{
    int rc = zello_client_send_text(c, text);
    cli_log("say: rc=%d (%s)", rc, rc == ZELLO_OK ? "ok" : "failed");
    return rc;
}

static char *trim(char *s)
{
    while (*s == ' ' || *s == '\t') s++;
    char *e = s + strlen(s);
    while (e > s && (e[-1] == '\n' || e[-1] == '\r' ||
                     e[-1] == ' '  || e[-1] == '\t'))
        *--e = 0;
    return s;
}

static void process_line(zello_client_t *c, char *raw)
{
    char *line = trim(raw);
    if (!*line) return;

    if (!strcmp(line, "help") || !strcmp(line, "?")) {
        cmd_help();
    } else if (!strcmp(line, "status")) {
        cmd_status(c);
    } else if (!strcmp(line, "quit") || !strcmp(line, "exit")) {
        g_quit = 1;
    } else if (!strncmp(line, "tone", 4) &&
               (line[4] == 0 || line[4] == ' ')) {
        int hz = 440, ms = 1500;
        if (line[4]) sscanf(line + 5, "%d %d", &hz, &ms);
        if (hz < 50 || hz > 8000)   { cli_log("hz must be 50-8000"); return; }
        if (ms < 100 || ms > 30000) { cli_log("ms must be 100-30000"); return; }
        cmd_tone(c, hz, ms);
    } else if (!strncmp(line, "wav ", 4)) {
        cmd_wav(c, line + 4);
    } else if (!strncmp(line, "say ", 4)) {
        cmd_say(c, line + 4);
    } else {
        cli_log("unknown: %s  (try 'help')", line);
    }
}

/* ── Command worker thread ─────────────────────────────────────── */

static void cmd_enqueue(const char *line)
{
    struct cmd_node *n = malloc(sizeof(*n));
    if (!n) return;
    n->line = strdup(line);
    n->next = NULL;
    if (!n->line) { free(n); return; }
    pthread_mutex_lock(&g_cmdq_mtx);
    if (!g_cmdq_tail) g_cmdq_tail = &g_cmdq_head;
    *g_cmdq_tail = n;
    g_cmdq_tail  = &n->next;
    pthread_cond_signal(&g_cmdq_cv);
    pthread_mutex_unlock(&g_cmdq_mtx);
}

static void *cmd_worker_fn(void *arg)
{
    zello_client_t *c = (zello_client_t *)arg;
    while (!g_quit) {
        pthread_mutex_lock(&g_cmdq_mtx);
        while (!g_cmdq_head && !g_quit)
            pthread_cond_wait(&g_cmdq_cv, &g_cmdq_mtx);
        struct cmd_node *n = g_cmdq_head;
        if (n) {
            g_cmdq_head = n->next;
            if (!g_cmdq_head) g_cmdq_tail = &g_cmdq_head;
        }
        pthread_mutex_unlock(&g_cmdq_mtx);
        if (!n) continue;
        process_line(c, n->line);
        free(n->line);
        free(n);
    }
    return NULL;
}

/* ── libzello service thread ───────────────────────────────────── */

static void *zello_service_thread_fn(void *arg)
{
    (void)arg;
    /* lws_service(ctx, 100) was observed taking up to 25 SECONDS on this
     * libwebsockets 4.3.5 — apparently the timeout isn't always honored.
     * Use timeout=0 (non-blocking service) and sleep externally so the
     * loop's cadence is fully under our control. 10 ms gives 100 Hz
     * which is fine for 60 ms Zello frames. */
    while (!g_quit) {
        if (g_cli) zello_client_poll(g_cli, 0);
        usleep(10000);
    }
    return NULL;
}

/* ── Main loop ──────────────────────────────────────────────────── */

int main(void)
{
    signal(SIGINT,  on_sigint);
    signal(SIGTERM, on_sigint);
    setvbuf(stdout, NULL, _IONBF, 0);

    fprintf(stderr, "libzello %s — interactive CLI\n", zello_version_string());
    if (getenv("ZELLO_CLI_QUIET") && *getenv("ZELLO_CLI_QUIET") == '1') {
        g_log_silent = 1;
        fprintf(stderr, "ZELLO_CLI_QUIET=1 — log output suppressed for diagnostic.\n");
    }

    zello_config_t cfg = {
        .server_url     = getenv("ZELLO_SERVER")
                          ? getenv("ZELLO_SERVER") : "wss://zello.io/ws",
        .username       = getenv("ZELLO_USERNAME"),
        .password       = getenv("ZELLO_PASSWORD"),
        .channel        = getenv("ZELLO_CHANNEL"),
        .auth_token     = getenv("ZELLO_AUTH_TOKEN"),
        .listen_only    = false,
        .tx_sample_rate = TX_RATE,
        .tx_frame_ms    = TX_FRAME_MS,
    };
    if (!cfg.username || !cfg.channel) {
        fprintf(stderr,
                "Set ZELLO_USERNAME and ZELLO_CHANNEL "
                "(and ZELLO_AUTH_TOKEN for F&F)\n");
        return 2;
    }
    snprintf(g_channel, sizeof(g_channel), "%s", cfg.channel);

    zello_callbacks_t cb = {
        .on_connected      = on_connected,
        .on_disconnected   = on_disconnected,
        .on_channel_status = on_channel_status,
        .on_stream_start   = on_stream_start,
        .on_audio          = on_audio,
        .on_stream_stop    = on_stream_stop,
        .on_text_message   = on_text_message,
        .on_error          = on_error,
    };

    /* Route everything libzello emits — including the libwebsockets
     * chatter it internally bridges — through cli_log so it coexists
     * with the linenoise prompt. Installed BEFORE create so setup-time
     * logs are wrapped too. */
    zello_set_log_callback(zello_log_handler, NULL);

    zello_client_t *c = zello_client_create(&cfg, &cb);
    if (!c) { fprintf(stderr, "create failed\n"); return 1; }
    g_cli = c;

    if (zello_client_start(c) != ZELLO_OK) {
        fprintf(stderr, "start failed\n");
        zello_client_destroy(c);
        return 1;
    }

    /* Spawn the dedicated WS service thread NOW so it drives the
     * connection handshake and waits for channel-ready. The main thread
     * just polls a flag. */
    if (pthread_create(&g_zello_thread, NULL, zello_service_thread_fn, NULL) != 0) {
        fprintf(stderr, "pthread_create (zello) failed\n");
        zello_client_destroy(c);
        return 1;
    }
    g_zello_thread_started = 1;

    /* Command worker — commands run here so the prompt returns instantly. */
    if (pthread_create(&g_cmd_thread, NULL, cmd_worker_fn, c) != 0) {
        fprintf(stderr, "pthread_create (cmd) failed\n");
        zello_client_destroy(c);
        return 1;
    }
    g_cmd_thread_started = 1;

    fprintf(stderr, "Waiting for channel ready (10s)...\n");
    long deadline = now_ms() + 10000;
    while (!g_connected && now_ms() < deadline && !g_quit) {
        log_flush_to_terminal();   /* show progress while connecting */
        usleep(50000);
    }
    log_flush_to_terminal();
    if (!g_connected) {
        fprintf(stderr, "timed out waiting for channel ready\n");
        zello_client_stop(c);
        usleep(500000);
        log_flush_to_terminal();
        zello_client_destroy(c);
        return 1;
    }

    fprintf(stderr, "Type 'help' for commands. Ctrl-D or 'quit' to exit.\n");
    /* Drain any logs queued during startup before the prompt appears. */
    log_flush_to_terminal();

    linenoiseHistorySetMaxLen(64);
    char editbuf[1024];
    linenoiseEditStart(&g_ls, STDIN_FILENO, STDOUT_FILENO,
                       editbuf, sizeof(editbuf), "zello> ");
    g_editing = 1;

    /* Main loop:
     *   - drain log queue (hide prompt, emit lines, redraw prompt)
     *   - wait up to 50 ms for stdin or for new logs to enqueue
     *   - on stdin: feed one byte to linenoise (instant since select said ready)
     *   - on full line: process the command
     * 50 ms is the maximum visual latency before a queued log line
     * appears on screen — well below human perception. The service
     * thread keeps polling lws_service in parallel; lws-side latency is
     * decoupled from this loop entirely. */
    while (!g_quit) {
        log_flush_to_terminal();

        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        struct timeval tv = { 0, 50 * 1000 };
        int ready = select(STDIN_FILENO + 1, &rfds, NULL, NULL, &tv);
        if (ready <= 0) continue;

        char *line = linenoiseEditFeed(&g_ls);
        if (line == linenoiseEditMore) continue;
        if (line == NULL) { g_quit = 1; break; }
        linenoiseEditStop(&g_ls);
        g_editing = 0;

        /* Drain again so any logs that fired during command processing
         * print on a clean line above the next prompt. */
        log_flush_to_terminal();

        if (*line) linenoiseHistoryAdd(line);
        /* Enqueue for the worker thread so the prompt comes back
         * immediately, even if the command takes seconds. */
        cmd_enqueue(line);
        linenoiseFree(line);
        log_flush_to_terminal();

        if (!g_quit) {
            linenoiseEditStart(&g_ls, STDIN_FILENO, STDOUT_FILENO,
                               editbuf, sizeof(editbuf), "zello> ");
            g_editing = 1;
        }
    }
    if (g_editing) { linenoiseEditStop(&g_ls); g_editing = 0; }

    fprintf(stderr, "\nDisconnecting...\n");
    g_quit = 1;
    /* Wake worker if blocked on the cmd queue cv. */
    pthread_mutex_lock(&g_cmdq_mtx);
    pthread_cond_broadcast(&g_cmdq_cv);
    pthread_mutex_unlock(&g_cmdq_mtx);
    zello_client_stop(c);
    if (g_cmd_thread_started)   pthread_join(g_cmd_thread, NULL);
    if (g_zello_thread_started) pthread_join(g_zello_thread, NULL);
    zello_client_destroy(c);
    g_cli = NULL;
    return 0;
}
