/*
 * zello_proto_priv.h — Internal protocol builders/parsers.
 *
 * Builders return malloc'd strings that the caller must free(). Parsers
 * accept a JSON object (cJSON*) and fill caller-provided structs.
 */

#ifndef LIBZELLO_PROTO_PRIV_H
#define LIBZELLO_PROTO_PRIV_H

#include "libzello/zello.h"
#include <cjson/cJSON.h>

/* Build the logon JSON for the given config and seq. Returns malloc'd
 * string on success, NULL on alloc failure. */
char *zello_build_logon(uint32_t seq,
                         const char *username,
                         const char *password,
                         const char *channel,
                         const char *auth_token,
                         const char *refresh_token,
                         bool listen_only);

/* Build start_stream message. codec_header is 4 raw bytes; this function
 * base64-encodes them inline. */
char *zello_build_start_stream(uint32_t seq,
                                const char *channel,
                                const uint8_t codec_header[4],
                                int packet_duration_ms);

char *zello_build_stop_stream(uint32_t seq,
                               uint32_t stream_id,
                               const char *channel);

char *zello_build_text_message(uint32_t seq,
                                const char *channel,
                                const char *text);

/* Parse a server message (already cJSON-parsed). Dispatches via callbacks
 * stored in the client. */
struct zello_client;
int zello_dispatch_message(struct zello_client *c, cJSON *root);

/* base64 helpers (RFC 4648). */
char *zello_b64_encode(const uint8_t *in, size_t n);
int   zello_b64_decode(const char *in, uint8_t *out, size_t out_max, size_t *out_n);

#endif
