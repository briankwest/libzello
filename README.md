# libzello

Zello Channel API client library in C. Pure protocol implementation against
`wss://zello.io/ws` (Zello Friends & Family) and `wss://zellowork.io/ws/{network}`
(Zello Work) — there is no official C or Linux SDK from Zello.

Built for embedded/server use cases like the [kerchunk](https://github.com/briankwest/kerchunk)
repeater controller, where a single Zello account bridges audio in and out of a
radio system.

## Status

Pre-alpha. Skeleton compiles; protocol not wired yet.

## Features (planned)

- WebSocket transport via libwebsockets (TLS via OpenSSL).
- Opus encode/decode at 16 kHz mono, 60 ms frames.
- Single channel, full duplex.
- `refresh_token`-based reconnect with exponential backoff.
- Polled API (caller drives the event loop) — no internal threads.

## Build

```sh
sudo apt install libwebsockets-dev libopus-dev libcjson-dev
./autogen.sh
./configure
make
sudo make install
```

## Quick API sketch

```c
#include <libzello/zello_client.h>

zello_config_t cfg = {
    .server_url = "wss://zello.io/ws",
    .username   = "alice",
    .password   = "...",
    .channel    = "MyChannel",
    .auth_token = "<JWT from developers.zello.com>",
};
zello_callbacks_t cb = { .on_audio = my_audio_cb, ... };

zello_client_t *c = zello_client_create(&cfg, &cb);
zello_client_start(c);
for (;;) zello_client_poll(c, 10);
```

## License

MIT — see [LICENSE](LICENSE).
