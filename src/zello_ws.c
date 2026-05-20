/*
 * zello_ws.c — WebSocket transport via libwebsockets.
 *
 * One libwebsockets context per zello_client. URL is parsed with
 * lws_parse_uri(). Outgoing frames are queued (libwebsockets only
 * permits lws_write() from inside the WRITEABLE callback) and drained
 * one per WRITEABLE event.
 *
 * All callbacks bounce back into the owning client via the entry points
 * declared in zello_client_internal.h (zello_on_*).
 */

#include "zello_ws.h"
#include "zello_internal.h"
#include "zello_log.h"

#include <libwebsockets.h>
#include <stdlib.h>
#include <string.h>

struct send_msg {
    uint8_t *buf;          /* LWS_PRE bytes of headroom + payload */
    size_t   len;          /* payload bytes (after LWS_PRE) */
    int      lws_flags;    /* LWS_WRITE_TEXT or LWS_WRITE_BINARY */
};

struct zello_ws {
    struct zello_client *owner;
    struct lws_context  *ctx;
    struct lws          *wsi;

    /* parsed URL — buffers are stable for the lifetime of zello_ws */
    char  url_buf[1024];        /* mutated by lws_parse_uri */
    const char *prot;
    const char *host;
    int   port;
    const char *path;           /* without leading '/' */
    char  path_with_slash[512]; /* libwebsockets wants leading '/' */
    bool  use_ssl;

    bool connected;
    bool want_close;

    /* send queue */
    struct send_msg *q;
    size_t q_n, q_cap;
};

static int ws_callback(struct lws *wsi, enum lws_callback_reasons reason,
                       void *user, void *in, size_t len);

static const struct lws_protocols PROTOCOLS[] = {
    {
        .name                  = "zello-channel-api",
        .callback              = ws_callback,
        .per_session_data_size = 0,
        .rx_buffer_size        = 65536,
        .id                    = 0,
        .user                  = NULL,
        .tx_packet_size        = 0,
    },
    /* Explicit NULL-name sentinel — portable across all libwebsockets
     * versions. LWS_PROTOCOL_LIST_TERM was added in 4.2; Debian 12
     * ships 4.0.20 and rejects the macro. */
    { NULL, NULL, 0, 0, 0, NULL, 0 }
};

/* ── send queue ─────────────────────────────────────────────────── */

static int queue_push(struct zello_ws *ws, const void *payload, size_t n, int flags)
{
    if (ws->q_n == ws->q_cap) {
        size_t ncap = ws->q_cap ? ws->q_cap * 2 : 8;
        struct send_msg *nq = realloc(ws->q, ncap * sizeof(*nq));
        if (!nq) return ZELLO_ERR_NOMEM;
        ws->q = nq;
        ws->q_cap = ncap;
    }
    struct send_msg *m = &ws->q[ws->q_n];
    m->buf = malloc(LWS_PRE + n);
    if (!m->buf) return ZELLO_ERR_NOMEM;
    memcpy(m->buf + LWS_PRE, payload, n);
    m->len = n;
    m->lws_flags = flags;
    ws->q_n++;
    if (ws->wsi) lws_callback_on_writable(ws->wsi);
    return ZELLO_OK;
}

static void queue_pop_front(struct zello_ws *ws)
{
    if (ws->q_n == 0) return;
    free(ws->q[0].buf);
    memmove(&ws->q[0], &ws->q[1], (ws->q_n - 1) * sizeof(ws->q[0]));
    ws->q_n--;
}

static void queue_free_all(struct zello_ws *ws)
{
    for (size_t i = 0; i < ws->q_n; i++) free(ws->q[i].buf);
    free(ws->q);
    ws->q = NULL;
    ws->q_n = ws->q_cap = 0;
}

/* ── lifecycle ──────────────────────────────────────────────────── */

struct zello_ws *zello_ws_create(struct zello_client *c)
{
    struct zello_ws *ws = calloc(1, sizeof(*ws));
    if (!ws) return NULL;
    ws->owner = c;

    struct lws_context_creation_info info = { 0 };
    info.port      = CONTEXT_PORT_NO_LISTEN;
    info.protocols = PROTOCOLS;
    info.options   = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.user      = ws;
    info.gid       = -1;
    info.uid       = -1;
    /* Default to errors+warnings only; ZELLO_WS_DEBUG=1 turns on the full
     * libwebsockets trace for debugging. */
    int lvl = LLL_ERR | LLL_WARN;
    const char *dbg = getenv("ZELLO_WS_DEBUG");
    if (dbg && *dbg == '1') lvl |= LLL_NOTICE | LLL_INFO | LLL_USER | LLL_CLIENT | LLL_HEADER;
    lws_set_log_level(lvl, NULL);

    ws->ctx = lws_create_context(&info);
    if (!ws->ctx) {
        free(ws);
        return NULL;
    }
    return ws;
}

void zello_ws_destroy(struct zello_ws *ws)
{
    if (!ws) return;
    queue_free_all(ws);
    if (ws->ctx) lws_context_destroy(ws->ctx);
    free(ws);
}

static int parse_url(struct zello_ws *ws, const char *url)
{
    if (!url || strlen(url) >= sizeof(ws->url_buf)) return ZELLO_ERR;
    strncpy(ws->url_buf, url, sizeof(ws->url_buf) - 1);
    ws->url_buf[sizeof(ws->url_buf) - 1] = 0;

    int port = 0;
    const char *prot = NULL, *host = NULL, *path = NULL;
    if (lws_parse_uri(ws->url_buf, &prot, &host, &port, &path) != 0) {
        ZLOG_E("zello_ws: failed to parse url '%s'", url);
        return ZELLO_ERR;
    }
    ws->prot = prot;
    ws->host = host;
    ws->path = path;
    ws->port = port;
    /* lws_parse_uri strips the leading '/'; libwebsockets connect wants it back. */
    snprintf(ws->path_with_slash, sizeof(ws->path_with_slash), "/%s", path ? path : "");
    ws->use_ssl = (prot && (!strcmp(prot, "wss") || !strcmp(prot, "https")));
    if (ws->port == 0) ws->port = ws->use_ssl ? 443 : 80;
    return ZELLO_OK;
}

int zello_ws_connect(struct zello_ws *ws, const char *url)
{
    if (!ws || !ws->ctx) return ZELLO_ERR;
    if (parse_url(ws, url) != ZELLO_OK) return ZELLO_ERR;

    struct lws_client_connect_info ci = { 0 };
    ci.context        = ws->ctx;
    ci.address        = ws->host;
    ci.port           = ws->port;
    ci.path           = ws->path_with_slash;
    ci.host           = ws->host;
    ci.origin         = ws->host;
    ci.protocol       = PROTOCOLS[0].name;
    ci.userdata       = ws;
    if (ws->use_ssl) {
        ci.ssl_connection = LCCSCF_USE_SSL;
    }

    ZLOG_I("zello_ws: connecting to %s://%s:%d%s",
           ws->prot, ws->host, ws->port, ws->path_with_slash);

    ws->wsi = lws_client_connect_via_info(&ci);
    if (!ws->wsi) {
        ZLOG_E("zello_ws: lws_client_connect_via_info failed");
        return ZELLO_ERR_NETWORK;
    }
    return ZELLO_OK;
}

int zello_ws_poll(struct zello_ws *ws, int timeout_ms)
{
    if (!ws || !ws->ctx) return 0;
    lws_service(ws->ctx, timeout_ms);
    return 0;
}

int zello_ws_send_text(struct zello_ws *ws, const char *json, size_t n)
{
    if (!ws || !json) return ZELLO_ERR;
    return queue_push(ws, json, n, LWS_WRITE_TEXT);
}

int zello_ws_send_binary(struct zello_ws *ws, const uint8_t *buf, size_t n)
{
    if (!ws || !buf) return ZELLO_ERR;
    return queue_push(ws, buf, n, LWS_WRITE_BINARY);
}

void zello_ws_close(struct zello_ws *ws)
{
    if (!ws) return;
    ws->want_close = true;
    if (ws->wsi) lws_callback_on_writable(ws->wsi);
}

bool zello_ws_is_connected(const struct zello_ws *ws)
{
    return ws && ws->connected;
}

/* ── libwebsockets callback ─────────────────────────────────────── */

static int ws_callback(struct lws *wsi, enum lws_callback_reasons reason,
                       void *user, void *in, size_t len)
{
    struct zello_ws *ws = (struct zello_ws *)user;

    switch (reason) {
    case LWS_CALLBACK_CLIENT_ESTABLISHED:
        if (!ws) break;
        ws->connected = true;
        zello_on_ws_established(ws->owner);
        break;

    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        if (!ws) break;
        ws->connected = false;
        ws->wsi = NULL;
        zello_on_ws_error(ws->owner, in ? (const char *)in : "connect failed");
        break;

    case LWS_CALLBACK_CLIENT_RECEIVE:
        if (!ws) break;
        /* libwebsockets may deliver a frame in multiple chunks; the final
         * chunk is signalled by lws_is_final_fragment(). For now we
         * assume each receive callback delivers a full frame (true for
         * the Zello control plane and for the audio payloads we care
         * about). A future enhancement should accumulate fragments. */
        zello_on_ws_message(ws->owner,
                            lws_frame_is_binary(wsi),
                            (const uint8_t *)in, len);
        break;

    case LWS_CALLBACK_CLIENT_WRITEABLE:
        if (!ws) break;
        if (ws->want_close) {
            return -1; /* causes lws to close the connection */
        }
        if (ws->q_n > 0) {
            struct send_msg *m = &ws->q[0];
            int n = lws_write(wsi, m->buf + LWS_PRE, m->len, m->lws_flags);
            if (n < (int)m->len) {
                ZLOG_W("zello_ws: lws_write short (%d/%zu)", n, m->len);
            }
            queue_pop_front(ws);
            if (ws->q_n > 0) lws_callback_on_writable(wsi);
        }
        break;

    case LWS_CALLBACK_CLIENT_CLOSED:
        if (!ws) break;
        ws->connected = false;
        ws->wsi = NULL;
        zello_on_ws_closed(ws->owner);
        break;

    default:
        break;
    }
    return 0;
}
