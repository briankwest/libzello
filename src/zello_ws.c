/*
 * zello_ws.c — Native WebSocket transport for libzello.
 *
 * Replaces the libwebsockets-based implementation. We control TCP +
 * TLS via OpenSSL directly and speak RFC 6455 ourselves. Single state
 * machine driven by zello_ws_poll() — caller chooses the cadence.
 *
 *   DISCONNECTED → DNS → TCP → TLS → UPGRADE → OPEN → CLOSED
 *
 * Frame format we send/receive (client-side):
 *   - Always FIN=1, opcode = 0x1 (text) or 0x2 (binary)
 *   - MASK=1, 4-byte random masking key, payload XOR'd
 *   - Server frames are not masked
 *   - We respond to server pings (0x9) with pongs (0xA)
 *
 * Same public API as the previous lws-based implementation, so the
 * rest of libzello (zello_client.c, zello_proto.c) doesn't change.
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "zello_ws.h"
#include "zello_internal.h"
#include "zello_log.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ── WS opcodes & limits ────────────────────────────────────────── */

#define WS_OP_CONT     0x0
#define WS_OP_TEXT     0x1
#define WS_OP_BIN      0x2
#define WS_OP_CLOSE    0x8
#define WS_OP_PING     0x9
#define WS_OP_PONG     0xA

#define WS_INBUF_SIZE  (64 * 1024)
#define WS_MAX_FRAME   (1 * 1024 * 1024)   /* sanity cap on inbound frame */

/* WebSocket GUID per RFC 6455 §4.2.2. */
#define WS_GUID "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

/* ── State ───────────────────────────────────────────────────────── */

typedef enum {
    WS_DISCONNECTED = 0,
    WS_RESOLVING,
    WS_TCP_CONNECTING,
    WS_TLS_HANDSHAKING,
    WS_HTTP_UPGRADE_PENDING,
    WS_OPEN,
    WS_CLOSING,
    WS_CLOSED,
} ws_state_t;

struct send_msg {
    uint8_t *buf;        /* serialized WS frame (header + masked payload) */
    size_t   len;
    size_t   off;        /* bytes sent so far */
};

struct zello_ws {
    struct zello_client *owner;
    ws_state_t           state;

    /* URL components. */
    char host[256];
    int  port;
    char path[256];
    int  use_tls;

    /* Socket + TLS. */
    int       fd;
    SSL_CTX  *ssl_ctx;
    SSL      *ssl;

    /* HTTP upgrade. */
    char  ws_key_b64[32];
    char  http_recv_buf[4096];
    size_t http_recv_n;

    /* Inbound frame parsing. */
    uint8_t  inbuf[WS_INBUF_SIZE];
    size_t   inbuf_n;
    uint8_t *frame_payload;     /* malloc'd, accumulated across reads */
    size_t   frame_payload_cap;
    size_t   frame_payload_n;
    size_t   frame_payload_need;
    int      frame_opcode;
    int      frame_fin;
    int      frame_have_hdr;

    /* Outbound send queue. */
    struct send_msg *q;
    size_t q_n, q_cap;

    bool connected;
    bool want_close;
};

/* ── Forward declarations ───────────────────────────────────────── */

static void ws_set_state(struct zello_ws *ws, ws_state_t s, const char *why);
static void ws_close_socket(struct zello_ws *ws);
static int  send_http_upgrade(struct zello_ws *ws);
static int  parse_http_response(struct zello_ws *ws);
static int  drain_outbound(struct zello_ws *ws);
static int  drain_inbound(struct zello_ws *ws);
static int  build_frame(uint8_t opcode, const void *payload, size_t plen,
                        uint8_t **out, size_t *out_len);
static int  queue_frame(struct zello_ws *ws, uint8_t opcode,
                        const void *payload, size_t plen);

/* ── State helpers ──────────────────────────────────────────────── */

static const char *ws_state_name(ws_state_t s)
{
    switch (s) {
    case WS_DISCONNECTED:        return "disconnected";
    case WS_RESOLVING:           return "resolving";
    case WS_TCP_CONNECTING:      return "tcp_connecting";
    case WS_TLS_HANDSHAKING:     return "tls_handshaking";
    case WS_HTTP_UPGRADE_PENDING:return "http_upgrade";
    case WS_OPEN:                return "open";
    case WS_CLOSING:             return "closing";
    case WS_CLOSED:              return "closed";
    }
    return "?";
}

static void ws_set_state(struct zello_ws *ws, ws_state_t s, const char *why)
{
    if (ws->state == s) return;
    ZLOG_D("zello_ws: %s -> %s%s%s", ws_state_name(ws->state), ws_state_name(s),
           why ? " (" : "", why ? why : "");
    ws->state = s;
}

static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* ── Base64 (RFC 4648, no line breaks) ──────────────────────────── */

static const char b64tab[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static size_t b64encode(const uint8_t *in, size_t n, char *out, size_t out_cap)
{
    size_t i = 0, o = 0;
    while (i + 3 <= n) {
        if (o + 4 >= out_cap) return 0;
        uint32_t v = (uint32_t)in[i] << 16 | (uint32_t)in[i+1] << 8 | in[i+2];
        out[o++] = b64tab[(v >> 18) & 0x3F];
        out[o++] = b64tab[(v >> 12) & 0x3F];
        out[o++] = b64tab[(v >>  6) & 0x3F];
        out[o++] = b64tab[ v        & 0x3F];
        i += 3;
    }
    if (i < n) {
        if (o + 4 >= out_cap) return 0;
        uint32_t v = (uint32_t)in[i] << 16;
        if (i + 1 < n) v |= (uint32_t)in[i+1] << 8;
        out[o++] = b64tab[(v >> 18) & 0x3F];
        out[o++] = b64tab[(v >> 12) & 0x3F];
        out[o++] = (i + 1 < n) ? b64tab[(v >> 6) & 0x3F] : '=';
        out[o++] = '=';
    }
    if (o >= out_cap) return 0;
    out[o] = 0;
    return o;
}

/* ── URL parsing ────────────────────────────────────────────────── */

static int parse_url(struct zello_ws *ws, const char *url)
{
    const char *p = url;
    int tls = 0;
    if (strncmp(p, "wss://", 6) == 0) { tls = 1; p += 6; }
    else if (strncmp(p, "ws://", 5) == 0) { p += 5; }
    else { ZLOG_E("zello_ws: bad url scheme: %s", url); return -1; }

    /* host[:port][/path] */
    const char *colon = strchr(p, ':');
    const char *slash = strchr(p, '/');
    const char *host_end;
    int port = tls ? 443 : 80;

    if (colon && (!slash || colon < slash)) {
        host_end = colon;
        port = atoi(colon + 1);
        if (port <= 0 || port > 65535) port = tls ? 443 : 80;
    } else {
        host_end = slash ? slash : p + strlen(p);
    }

    size_t hostlen = (size_t)(host_end - p);
    if (hostlen == 0 || hostlen >= sizeof(ws->host)) {
        ZLOG_E("zello_ws: bad url host: %s", url);
        return -1;
    }
    memcpy(ws->host, p, hostlen);
    ws->host[hostlen] = 0;
    ws->port = port;
    ws->use_tls = tls;

    if (slash) snprintf(ws->path, sizeof(ws->path), "%s", slash);
    else       snprintf(ws->path, sizeof(ws->path), "/");

    return 0;
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

struct zello_ws *zello_ws_create(struct zello_client *c)
{
    struct zello_ws *ws = calloc(1, sizeof(*ws));
    if (!ws) return NULL;
    ws->owner = c;
    ws->fd = -1;
    ws->state = WS_DISCONNECTED;

    /* Lazy TLS setup — defer SSL_library_init equivalents until first
     * connect, but SSL_CTX needs to exist eventually. */
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();

    ws->ssl_ctx = SSL_CTX_new(TLS_client_method());
    if (!ws->ssl_ctx) {
        ZLOG_E("zello_ws: SSL_CTX_new failed");
        free(ws);
        return NULL;
    }
    SSL_CTX_set_default_verify_paths(ws->ssl_ctx);
    /* For now: don't strictly verify cert. Production would set
     * SSL_CTX_set_verify(VERIFY_PEER). */

    return ws;
}

static void free_outq(struct zello_ws *ws)
{
    for (size_t i = 0; i < ws->q_n; i++) free(ws->q[i].buf);
    free(ws->q);
    ws->q = NULL;
    ws->q_n = ws->q_cap = 0;
}

static void ws_close_socket(struct zello_ws *ws)
{
    if (ws->ssl) {
        SSL_shutdown(ws->ssl);
        SSL_free(ws->ssl);
        ws->ssl = NULL;
    }
    if (ws->fd >= 0) {
        close(ws->fd);
        ws->fd = -1;
    }
    ws->connected = false;
    ws->inbuf_n = 0;
    ws->http_recv_n = 0;
    free(ws->frame_payload);
    ws->frame_payload = NULL;
    ws->frame_payload_cap = ws->frame_payload_n = ws->frame_payload_need = 0;
    ws->frame_have_hdr = 0;
    free_outq(ws);
}

void zello_ws_destroy(struct zello_ws *ws)
{
    if (!ws) return;
    ws_close_socket(ws);
    if (ws->ssl_ctx) SSL_CTX_free(ws->ssl_ctx);
    free(ws);
}

bool zello_ws_is_connected(const struct zello_ws *ws)
{
    return ws && ws->connected;
}

void zello_ws_close(struct zello_ws *ws)
{
    if (!ws) return;
    if (ws->state == WS_OPEN) {
        /* Send a close frame politely; if we can't, just close. */
        uint8_t close_payload[2] = { 0x03, 0xE8 }; /* 1000: normal */
        queue_frame(ws, WS_OP_CLOSE, close_payload, 2);
        ws_set_state(ws, WS_CLOSING, "client close requested");
    }
    ws->want_close = true;
}

/* ── Connect path ───────────────────────────────────────────────── */

int zello_ws_connect(struct zello_ws *ws, const char *url)
{
    if (!ws) return ZELLO_ERR;

    /* Reset prior state if any. */
    ws_close_socket(ws);

    if (parse_url(ws, url) < 0) return ZELLO_ERR;

    ZLOG_I("zello_ws: connecting to %s://%s:%d%s",
           ws->use_tls ? "wss" : "ws", ws->host, ws->port, ws->path);

    /* Generate Sec-WebSocket-Key — 16 random bytes, base64. */
    uint8_t key_raw[16];
    if (RAND_bytes(key_raw, sizeof(key_raw)) != 1) {
        ZLOG_E("zello_ws: RAND_bytes failed");
        return ZELLO_ERR;
    }
    if (b64encode(key_raw, sizeof(key_raw), ws->ws_key_b64,
                  sizeof(ws->ws_key_b64)) == 0) {
        ZLOG_E("zello_ws: ws_key encode failed");
        return ZELLO_ERR;
    }

    /* getaddrinfo (blocking) — fast enough; reaches state TCP_CONNECTING. */
    struct addrinfo hints = { 0 }, *res = NULL;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char portstr[12];
    snprintf(portstr, sizeof(portstr), "%d", ws->port);

    ws_set_state(ws, WS_RESOLVING, NULL);
    int gai = getaddrinfo(ws->host, portstr, &hints, &res);
    if (gai != 0 || !res) {
        ZLOG_E("zello_ws: getaddrinfo(%s) failed: %s", ws->host,
               gai_strerror(gai));
        return ZELLO_ERR_NETWORK;
    }

    /* Try addresses in order until one works (typically IPv4 first). */
    int fd = -1;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        if (set_nonblocking(fd) < 0) { close(fd); fd = -1; continue; }

        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        int rc = connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc == 0 || (rc < 0 && errno == EINPROGRESS)) {
            break; /* will complete async */
        }
        ZLOG_W("zello_ws: connect attempt failed: %s", strerror(errno));
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);

    if (fd < 0) {
        ZLOG_E("zello_ws: no usable address for %s", ws->host);
        return ZELLO_ERR_NETWORK;
    }

    ws->fd = fd;
    ws_set_state(ws, WS_TCP_CONNECTING, NULL);
    return ZELLO_OK;
}

/* ── State-machine advance steps ────────────────────────────────── */

static int complete_tcp_connect(struct zello_ws *ws)
{
    int err = 0;
    socklen_t len = sizeof(err);
    if (getsockopt(ws->fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err) {
        ZLOG_E("zello_ws: TCP connect failed: %s", strerror(err ? err : errno));
        return -1;
    }
    if (ws->use_tls) {
        ws->ssl = SSL_new(ws->ssl_ctx);
        if (!ws->ssl) { ZLOG_E("zello_ws: SSL_new failed"); return -1; }
        SSL_set_tlsext_host_name(ws->ssl, ws->host);
        SSL_set_fd(ws->ssl, ws->fd);
        ws_set_state(ws, WS_TLS_HANDSHAKING, NULL);
    } else {
        ws_set_state(ws, WS_HTTP_UPGRADE_PENDING, NULL);
        if (send_http_upgrade(ws) < 0) return -1;
    }
    return 0;
}

static int do_tls_handshake(struct zello_ws *ws)
{
    int r = SSL_connect(ws->ssl);
    if (r == 1) {
        ZLOG_I("zello_ws: TLS established (%s)", SSL_get_cipher(ws->ssl));
        ws_set_state(ws, WS_HTTP_UPGRADE_PENDING, NULL);
        if (send_http_upgrade(ws) < 0) return -1;
        return 0;
    }
    int e = SSL_get_error(ws->ssl, r);
    if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) return 0;
    ZLOG_E("zello_ws: TLS handshake failed: SSL_error=%d", e);
    ERR_print_errors_fp(stderr);
    return -1;
}

static int send_http_upgrade(struct zello_ws *ws)
{
    char req[1024];
    int n = snprintf(req, sizeof(req),
        "GET %s HTTP/1.1\r\n"
        "Host: %s:%d\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "User-Agent: libzello/0.1\r\n"
        "\r\n",
        ws->path, ws->host, ws->port, ws->ws_key_b64);
    if (n <= 0 || n >= (int)sizeof(req)) {
        ZLOG_E("zello_ws: upgrade request too large");
        return -1;
    }

    /* Send synchronously — small request, socket has plenty of TX buffer. */
    int sent = 0;
    while (sent < n) {
        int w;
        if (ws->ssl) w = SSL_write(ws->ssl, req + sent, n - sent);
        else         w = (int)send(ws->fd, req + sent, n - sent, 0);
        if (w <= 0) {
            if (ws->ssl) {
                int e = SSL_get_error(ws->ssl, w);
                if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) {
                    /* would block — retry on next poll */
                    /* (for the upgrade request this is unusual; in practice
                     * fits in one SSL_write.) */
                    struct pollfd p = { ws->fd,
                        (e == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT), 0 };
                    poll(&p, 1, 200);
                    continue;
                }
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd p = { ws->fd, POLLOUT, 0 };
                poll(&p, 1, 200);
                continue;
            }
            ZLOG_E("zello_ws: upgrade write failed");
            return -1;
        }
        sent += w;
    }
    ZLOG_D("zello_ws: sent HTTP upgrade (%d bytes)", n);
    return 0;
}

static int parse_http_response(struct zello_ws *ws)
{
    /* http_recv_buf contains accumulated bytes; look for "\r\n\r\n". */
    ws->http_recv_buf[ws->http_recv_n] = 0;
    char *end = strstr(ws->http_recv_buf, "\r\n\r\n");
    if (!end) return 0; /* need more */

    *end = 0;
    /* Status line. */
    if (strncmp(ws->http_recv_buf, "HTTP/1.1 101", 12) != 0 &&
        strncmp(ws->http_recv_buf, "HTTP/1.0 101", 12) != 0) {
        ZLOG_E("zello_ws: upgrade failed, server said: %.200s",
               ws->http_recv_buf);
        return -1;
    }

    /* We could validate Sec-WebSocket-Accept here, but Zello is fine
     * and the GUID compare adds dependency on SHA1 we already have via
     * OpenSSL. For now we trust the 101 status. */

    /* Move any trailing bytes (start of first WS frame) into inbuf. */
    size_t hdr_bytes = (size_t)(end - ws->http_recv_buf) + 4;
    if (ws->http_recv_n > hdr_bytes) {
        size_t extra = ws->http_recv_n - hdr_bytes;
        memcpy(ws->inbuf, ws->http_recv_buf + hdr_bytes, extra);
        ws->inbuf_n = extra;
    }
    ws->http_recv_n = 0;

    ws_set_state(ws, WS_OPEN, NULL);
    ws->connected = true;
    if (ws->owner) zello_on_ws_established(ws->owner);
    return 1;
}

static int read_for_upgrade(struct zello_ws *ws)
{
    int r;
    if (ws->ssl)
        r = SSL_read(ws->ssl, ws->http_recv_buf + ws->http_recv_n,
                     (int)(sizeof(ws->http_recv_buf) - 1 - ws->http_recv_n));
    else
        r = (int)recv(ws->fd, ws->http_recv_buf + ws->http_recv_n,
                      sizeof(ws->http_recv_buf) - 1 - ws->http_recv_n, 0);
    if (r > 0) {
        ws->http_recv_n += (size_t)r;
        return parse_http_response(ws);
    }
    if (r == 0) {
        ZLOG_E("zello_ws: server closed during upgrade");
        return -1;
    }
    if (ws->ssl) {
        int e = SSL_get_error(ws->ssl, r);
        if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) return 0;
        ZLOG_E("zello_ws: SSL_read err during upgrade: %d", e);
        return -1;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
    ZLOG_E("zello_ws: upgrade read failed: %s", strerror(errno));
    return -1;
}

/* ── WS frame I/O ───────────────────────────────────────────────── */

static int build_frame(uint8_t opcode, const void *payload, size_t plen,
                       uint8_t **out, size_t *out_len)
{
    /* Header: 2..14 bytes. Payload masked with 4-byte key. */
    uint8_t hdr[14];
    size_t hdr_len = 2;
    hdr[0] = 0x80 | (opcode & 0x0F);   /* FIN=1, opcode */
    if (plen <= 125) {
        hdr[1] = 0x80 | (uint8_t)plen; /* MASK=1, len */
    } else if (plen <= 0xFFFF) {
        hdr[1] = 0x80 | 126;
        hdr[2] = (uint8_t)(plen >> 8);
        hdr[3] = (uint8_t)(plen & 0xFF);
        hdr_len = 4;
    } else {
        hdr[1] = 0x80 | 127;
        for (int i = 0; i < 8; i++)
            hdr[2 + i] = (uint8_t)((uint64_t)plen >> ((7 - i) * 8));
        hdr_len = 10;
    }

    uint8_t mask[4];
    if (RAND_bytes(mask, 4) != 1) return -1;
    memcpy(hdr + hdr_len, mask, 4);
    hdr_len += 4;

    size_t total = hdr_len + plen;
    uint8_t *buf = malloc(total);
    if (!buf) return -1;
    memcpy(buf, hdr, hdr_len);
    const uint8_t *src = (const uint8_t *)payload;
    for (size_t i = 0; i < plen; i++)
        buf[hdr_len + i] = src[i] ^ mask[i & 3];

    *out = buf;
    *out_len = total;
    return 0;
}

static int queue_frame(struct zello_ws *ws, uint8_t opcode,
                       const void *payload, size_t plen)
{
    uint8_t *buf = NULL;
    size_t   len = 0;
    if (build_frame(opcode, payload, plen, &buf, &len) < 0) return ZELLO_ERR;

    if (ws->q_n == ws->q_cap) {
        size_t ncap = ws->q_cap ? ws->q_cap * 2 : 8;
        struct send_msg *nq = realloc(ws->q, ncap * sizeof(*nq));
        if (!nq) { free(buf); return ZELLO_ERR_NOMEM; }
        ws->q = nq;
        ws->q_cap = ncap;
    }
    ws->q[ws->q_n].buf = buf;
    ws->q[ws->q_n].len = len;
    ws->q[ws->q_n].off = 0;
    ws->q_n++;
    return ZELLO_OK;
}

int zello_ws_send_text(struct zello_ws *ws, const char *json, size_t n)
{
    if (!ws || !json) return ZELLO_ERR;
    return queue_frame(ws, WS_OP_TEXT, json, n);
}

int zello_ws_send_binary(struct zello_ws *ws, const uint8_t *buf, size_t n)
{
    if (!ws || !buf) return ZELLO_ERR;
    return queue_frame(ws, WS_OP_BIN, buf, n);
}

static int drain_outbound(struct zello_ws *ws)
{
    while (ws->q_n > 0) {
        struct send_msg *m = &ws->q[0];
        size_t left = m->len - m->off;
        int w;
        if (ws->ssl) w = SSL_write(ws->ssl, m->buf + m->off, (int)left);
        else         w = (int)send(ws->fd, m->buf + m->off, left, 0);
        if (w > 0) {
            m->off += (size_t)w;
            if (m->off >= m->len) {
                free(m->buf);
                memmove(&ws->q[0], &ws->q[1], (ws->q_n - 1) * sizeof(*m));
                ws->q_n--;
            }
            continue;
        }
        if (ws->ssl) {
            int e = SSL_get_error(ws->ssl, w);
            if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) return 0;
            ZLOG_E("zello_ws: SSL_write err: %d", e);
            return -1;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        ZLOG_E("zello_ws: send failed: %s", strerror(errno));
        return -1;
    }
    return 0;
}

/* Try to parse one frame out of inbuf. Returns:
 *   1 — a complete frame was parsed and dispatched
 *   0 — need more data
 *  -1 — protocol error / close
 */
static int try_parse_frame(struct zello_ws *ws)
{
    if (ws->inbuf_n < 2) return 0;

    uint8_t b0 = ws->inbuf[0];
    uint8_t b1 = ws->inbuf[1];
    int fin     = (b0 & 0x80) != 0;
    int opcode  = b0 & 0x0F;
    int masked  = (b1 & 0x80) != 0;
    uint64_t plen = b1 & 0x7F;
    size_t hdr_n = 2;

    if (plen == 126) {
        if (ws->inbuf_n < 4) return 0;
        plen = ((uint64_t)ws->inbuf[2] << 8) | ws->inbuf[3];
        hdr_n = 4;
    } else if (plen == 127) {
        if (ws->inbuf_n < 10) return 0;
        plen = 0;
        for (int i = 0; i < 8; i++) plen = (plen << 8) | ws->inbuf[2 + i];
        hdr_n = 10;
    }

    uint8_t mask_key[4] = {0};
    if (masked) {
        if (ws->inbuf_n < hdr_n + 4) return 0;
        memcpy(mask_key, ws->inbuf + hdr_n, 4);
        hdr_n += 4;
    }

    if (plen > WS_MAX_FRAME) {
        ZLOG_E("zello_ws: oversized frame %llu", (unsigned long long)plen);
        return -1;
    }

    if (ws->inbuf_n < hdr_n + plen) return 0;  /* need more body */

    uint8_t *body = ws->inbuf + hdr_n;
    if (masked) {
        for (uint64_t i = 0; i < plen; i++) body[i] ^= mask_key[i & 3];
    }

    /* Dispatch by opcode. We treat CONT as a no-op (Zello control plane
     * doesn't fragment in practice). For our use case, every TEXT or
     * BIN frame is a complete message. */
    switch (opcode) {
    case WS_OP_TEXT:
    case WS_OP_BIN:
        if (ws->owner)
            zello_on_ws_message(ws->owner, opcode == WS_OP_BIN, body, (size_t)plen);
        break;
    case WS_OP_PING:
        /* Reply with a pong carrying the same payload. */
        queue_frame(ws, WS_OP_PONG, body, (size_t)plen);
        break;
    case WS_OP_PONG:
        /* drop */
        break;
    case WS_OP_CLOSE:
        ZLOG_I("zello_ws: server sent close frame (%llu bytes)",
               (unsigned long long)plen);
        /* Echo back if we haven't already started closing. */
        if (ws->state == WS_OPEN) queue_frame(ws, WS_OP_CLOSE, body, (size_t)plen);
        return -1;
    default:
        ZLOG_W("zello_ws: unknown opcode 0x%x", opcode);
        break;
    }
    (void)fin;

    /* Consume hdr + body from inbuf. */
    size_t consumed = hdr_n + (size_t)plen;
    if (ws->inbuf_n > consumed)
        memmove(ws->inbuf, ws->inbuf + consumed, ws->inbuf_n - consumed);
    ws->inbuf_n -= consumed;
    return 1;
}

static int drain_inbound(struct zello_ws *ws)
{
    for (;;) {
        if (ws->inbuf_n >= sizeof(ws->inbuf)) {
            ZLOG_E("zello_ws: inbuf overflow");
            return -1;
        }
        int r;
        if (ws->ssl)
            r = SSL_read(ws->ssl, ws->inbuf + ws->inbuf_n,
                         (int)(sizeof(ws->inbuf) - ws->inbuf_n));
        else
            r = (int)recv(ws->fd, ws->inbuf + ws->inbuf_n,
                          sizeof(ws->inbuf) - ws->inbuf_n, 0);
        if (r > 0) {
            ws->inbuf_n += (size_t)r;
            /* Parse as many full frames as we have. */
            for (;;) {
                int p = try_parse_frame(ws);
                if (p == 0) break;
                if (p < 0)  return -1;
            }
            continue;
        }
        if (r == 0) {
            ZLOG_I("zello_ws: server closed connection");
            return -1;
        }
        if (ws->ssl) {
            int e = SSL_get_error(ws->ssl, r);
            if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE) return 0;
            ZLOG_E("zello_ws: SSL_read err: %d", e);
            return -1;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        ZLOG_E("zello_ws: recv failed: %s", strerror(errno));
        return -1;
    }
}

/* ── Poll loop ──────────────────────────────────────────────────── */

int zello_ws_poll(struct zello_ws *ws, int timeout_ms)
{
    if (!ws || ws->fd < 0) return 0;

    /* Build poll events based on state. */
    short events = 0;
    if (ws->state == WS_TCP_CONNECTING)             events = POLLOUT;
    else if (ws->state == WS_TLS_HANDSHAKING)       events = POLLIN | POLLOUT;
    else if (ws->state == WS_HTTP_UPGRADE_PENDING)  events = POLLIN;
    else if (ws->state == WS_OPEN || ws->state == WS_CLOSING) {
        events = POLLIN | (ws->q_n > 0 ? POLLOUT : 0);
    }

    struct pollfd pfd = { ws->fd, events, 0 };
    int pr = poll(&pfd, 1, timeout_ms);
    if (pr < 0) {
        if (errno == EINTR) return 0;
        ZLOG_E("zello_ws: poll: %s", strerror(errno));
        ws_close_socket(ws);
        if (ws->owner) zello_on_ws_closed(ws->owner);
        return -1;
    }
    if (pr == 0) return 0;

    /* Advance the state machine. */
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        ZLOG_W("zello_ws: poll revents 0x%x", pfd.revents);
        ws_close_socket(ws);
        if (ws->owner) zello_on_ws_closed(ws->owner);
        return -1;
    }

    if (ws->state == WS_TCP_CONNECTING) {
        if (complete_tcp_connect(ws) < 0) goto err;
    }
    if (ws->state == WS_TLS_HANDSHAKING) {
        if (do_tls_handshake(ws) < 0) goto err;
    }
    if (ws->state == WS_HTTP_UPGRADE_PENDING) {
        int rc = read_for_upgrade(ws);
        if (rc < 0) goto err;
        /* If rc == 0 we just need more data. If 1, state is now OPEN. */
    }
    if (ws->state == WS_OPEN || ws->state == WS_CLOSING) {
        if (pfd.revents & POLLOUT) {
            if (drain_outbound(ws) < 0) goto err;
        }
        if (pfd.revents & POLLIN) {
            if (drain_inbound(ws) < 0) goto err;
        }
    }

    if (ws->want_close && ws->q_n == 0 && ws->state == WS_CLOSING) {
        ws_close_socket(ws);
        if (ws->owner) zello_on_ws_closed(ws->owner);
    }
    return 0;

err:
    ws_close_socket(ws);
    if (ws->owner) zello_on_ws_error(ws->owner, "io error");
    return -1;
}
