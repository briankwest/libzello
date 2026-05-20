#ifndef LIBZELLO_WS_H
#define LIBZELLO_WS_H

#include "libzello/zello.h"

struct zello_client;
struct zello_ws;

struct zello_ws *zello_ws_create(struct zello_client *c);
void             zello_ws_destroy(struct zello_ws *ws);

int  zello_ws_connect(struct zello_ws *ws, const char *url);
int  zello_ws_poll   (struct zello_ws *ws, int timeout_ms);
int  zello_ws_send_text  (struct zello_ws *ws, const char *json, size_t n);
int  zello_ws_send_binary(struct zello_ws *ws, const uint8_t *buf, size_t n);
void zello_ws_close  (struct zello_ws *ws);
bool zello_ws_is_connected(const struct zello_ws *ws);

#endif
