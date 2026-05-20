/*
 * zello_proto.c — JSON builders/parsers for the Zello channel control plane.
 *
 * Builders: produce malloc'd JSON strings the caller frees.
 * Dispatcher: parses an incoming server JSON message and fires the
 * matching public callback on the owning zello_client.
 *
 * Reference: https://github.com/zelloptt/zello-channel-api/blob/main/API.md
 */

#include "zello_proto_priv.h"
#include "zello_internal.h"
#include "zello_log.h"
#include "libzello/zello_client.h"

#include <libwebsockets.h>          /* for lws_b64_* */
#include <cjson/cJSON.h>
#include <stdlib.h>
#include <string.h>

/* ── builders ───────────────────────────────────────────────────── */

static char *json_render_and_free(cJSON *root)
{
    if (!root) return NULL;
    char *out = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return out;
}

char *zello_build_logon(uint32_t seq,
                         const char *username,
                         const char *password,
                         const char *channel,
                         const char *auth_token,
                         const char *refresh_token,
                         bool listen_only)
{
    cJSON *r = cJSON_CreateObject();
    if (!r) return NULL;
    cJSON_AddStringToObject(r, "command", "logon");
    cJSON_AddNumberToObject(r, "seq", (double)seq);
    if (auth_token && *auth_token)
        cJSON_AddStringToObject(r, "auth_token", auth_token);
    if (refresh_token && *refresh_token)
        cJSON_AddStringToObject(r, "refresh_token", refresh_token);
    if (username && *username)
        cJSON_AddStringToObject(r, "username", username);
    /* On refresh-token reconnects we omit password (server uses refresh_token). */
    if (password && *password && !(refresh_token && *refresh_token))
        cJSON_AddStringToObject(r, "password", password);
    if (channel && *channel) {
        cJSON *channels = cJSON_AddArrayToObject(r, "channels");
        cJSON_AddItemToArray(channels, cJSON_CreateString(channel));
    }
    /* Always send listen_only explicitly — the server is sensitive to
     * its presence (omitting + non-listen mode causes silent drop). */
    cJSON_AddBoolToObject(r, "listen_only", listen_only ? 1 : 0);
    /* Platform identification — Zello accepts arbitrary strings here but
     * appears to silently drop full-mode logons that omit them entirely. */
    cJSON_AddStringToObject(r, "version",       "libzello/0.1.0");
    cJSON_AddStringToObject(r, "platform_type", "linux");
    cJSON_AddStringToObject(r, "platform_name", "kerchunkd");
    cJSON_AddStringToObject(r, "language",      "en");
    return json_render_and_free(r);
}

char *zello_build_start_stream(uint32_t seq,
                                const char *channel,
                                const uint8_t codec_header[4],
                                int packet_duration_ms)
{
    cJSON *r = cJSON_CreateObject();
    if (!r) return NULL;
    cJSON_AddStringToObject(r, "command", "start_stream");
    cJSON_AddNumberToObject(r, "seq", (double)seq);
    if (channel && *channel)
        cJSON_AddStringToObject(r, "channel", channel);
    cJSON_AddStringToObject(r, "type",  "audio");
    cJSON_AddStringToObject(r, "codec", "opus");
    char *b64 = zello_b64_encode(codec_header, 4);
    if (b64) {
        cJSON_AddStringToObject(r, "codec_header", b64);
        free(b64);
    }
    cJSON_AddNumberToObject(r, "packet_duration", (double)packet_duration_ms);
    return json_render_and_free(r);
}

char *zello_build_stop_stream(uint32_t seq, uint32_t stream_id, const char *channel)
{
    cJSON *r = cJSON_CreateObject();
    if (!r) return NULL;
    cJSON_AddStringToObject(r, "command", "stop_stream");
    cJSON_AddNumberToObject(r, "seq", (double)seq);
    cJSON_AddNumberToObject(r, "stream_id", (double)stream_id);
    if (channel && *channel)
        cJSON_AddStringToObject(r, "channel", channel);
    return json_render_and_free(r);
}

char *zello_build_text_message(uint32_t seq, const char *channel, const char *text)
{
    cJSON *r = cJSON_CreateObject();
    if (!r) return NULL;
    cJSON_AddStringToObject(r, "command", "send_text_message");
    cJSON_AddNumberToObject(r, "seq", (double)seq);
    if (channel && *channel)
        cJSON_AddStringToObject(r, "channel", channel);
    cJSON_AddStringToObject(r, "text", text ? text : "");
    return json_render_and_free(r);
}

/* ── base64 (wraps libwebsockets) ───────────────────────────────── */

char *zello_b64_encode(const uint8_t *in, size_t n)
{
    if (!in) return NULL;
    /* Standard b64 expands by 4/3, plus padding & NUL. */
    size_t out_max = ((n + 2) / 3) * 4 + 4;
    char *out = malloc(out_max);
    if (!out) return NULL;
    int written = lws_b64_encode_string((const char *)in, (int)n, out, (int)out_max);
    if (written < 0) { free(out); return NULL; }
    return out;
}

int zello_b64_decode(const char *in, uint8_t *out, size_t out_max, size_t *out_n)
{
    if (!in || !out) return ZELLO_ERR;
    int n = lws_b64_decode_string(in, (char *)out, (int)out_max);
    if (n < 0) return ZELLO_ERR;
    if (out_n) *out_n = (size_t)n;
    return ZELLO_OK;
}

/* ── dispatcher ─────────────────────────────────────────────────── */

static const char *json_get_string(const cJSON *o, const char *k)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    return cJSON_IsString(v) ? v->valuestring : NULL;
}

static int json_get_int(const cJSON *o, const char *k, int dflt)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    if (cJSON_IsNumber(v)) return (int)v->valuedouble;
    return dflt;
}

static bool json_get_bool(const cJSON *o, const char *k, bool dflt)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    if (cJSON_IsBool(v)) return cJSON_IsTrue(v);
    if (cJSON_IsString(v)) return strcmp(v->valuestring, "online") == 0;
    return dflt;
}

/* Implemented in zello_client.c — gives the dispatcher access to the
 * client state for stashing refresh_token / firing callbacks. */
void zello_client_handle_logon_response       (zello_client_t *c, const cJSON *root);
void zello_client_handle_start_stream_response(zello_client_t *c, const cJSON *root);
void zello_client_handle_on_stream_start      (zello_client_t *c, const cJSON *root);
void zello_client_handle_on_stream_stop       (zello_client_t *c, const cJSON *root);

int zello_dispatch_message(struct zello_client *c, cJSON *root)
{
    if (!c || !root) return ZELLO_ERR;

    /* Responses to our outgoing commands echo `seq` and lack `command`. */
    const cJSON *seq_v = cJSON_GetObjectItemCaseSensitive(root, "seq");
    const cJSON *cmd_v = cJSON_GetObjectItemCaseSensitive(root, "command");

    if (cJSON_IsNumber(seq_v) && !cJSON_IsString(cmd_v)) {
        uint32_t seq = (uint32_t)seq_v->valuedouble;
        if (seq == c->pending_logon_seq) {
            zello_client_handle_logon_response(c, root);
            return ZELLO_OK;
        }
        if (seq == c->pending_start_stream_seq) {
            zello_client_handle_start_stream_response(c, root);
            return ZELLO_OK;
        }
        /* Other command responses (stop_stream, text_message, …): log
         * any server-reported error; the success ack carries no payload
         * we need to act on. */
        const char *err = json_get_string(root, "error");
        if (err) {
            ZLOG_W("zello: command seq=%u error: %s", seq, err);
            if (c->cb.on_error) c->cb.on_error(c, err, c->cb.userdata);
        } else {
            ZLOG_D("zello: command seq=%u ok", seq);
        }
        return ZELLO_OK;
    }

    if (!cJSON_IsString(cmd_v)) {
        ZLOG_W("zello: server message with no command");
        return ZELLO_ERR_PROTOCOL;
    }

    const char *cmd = cmd_v->valuestring;

    if (strcmp(cmd, "on_channel_status") == 0) {
        const char *chan = json_get_string(root, "channel");
        bool online      = json_get_bool(root, "status", false);
        int users        = json_get_int (root, "users_online", 0);

        /* First online for our configured channel = "ready to TX".
         * Fire on_connected here (not on the logon response) — start_stream
         * is rejected with "channel is not ready" until this point. */
        bool first_ready = false;
        if (online && chan && c->cfg.channel &&
            strcmp(chan, c->cfg.channel) == 0 && !c->channel_ready) {
            c->channel_ready = true;
            first_ready = true;
        } else if (!online && chan && c->cfg.channel &&
                   strcmp(chan, c->cfg.channel) == 0) {
            c->channel_ready = false;
        }

        if (c->cb.on_channel_status)
            c->cb.on_channel_status(c, chan, online, users, c->cb.userdata);

        if (first_ready && c->cb.on_connected)
            c->cb.on_connected(c, c->refresh_token, c->cb.userdata);
        return ZELLO_OK;
    }

    if (strcmp(cmd, "on_stream_start") == 0) {
        zello_client_handle_on_stream_start(c, root);
        return ZELLO_OK;
    }

    if (strcmp(cmd, "on_stream_stop") == 0) {
        zello_client_handle_on_stream_stop(c, root);
        return ZELLO_OK;
    }

    if (strcmp(cmd, "on_text_message") == 0) {
        const char *from = json_get_string(root, "from");
        const char *text = json_get_string(root, "text");
        if (c->cb.on_text_message)
            c->cb.on_text_message(c, from, text, c->cb.userdata);
        return ZELLO_OK;
    }

    if (strcmp(cmd, "on_error") == 0) {
        const char *err = json_get_string(root, "error");
        ZLOG_W("zello: on_error: %s", err ? err : "(no detail)");
        if (c->cb.on_error) c->cb.on_error(c, err, c->cb.userdata);
        return ZELLO_OK;
    }

    ZLOG_D("zello: unhandled command '%s'", cmd);
    return ZELLO_OK;
}
