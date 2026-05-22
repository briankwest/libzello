# libzello

Zello Channel API client library in C. Pure protocol implementation against
`wss://zello.io/ws` (Zello Friends & Family) and `wss://zellowork.io/ws/{network}`
(Zello Work) — there is no official C or Linux SDK from Zello.

Built for embedded/server use cases like the [kerchunk](https://github.com/briankwest/kerchunk)
repeater controller, where a single Zello account bridges audio in and out of a
radio system.

## Status

Working. v0.1.6 is the current release. End-to-end TX and RX have been
exercised against the live `zello.io` server with real listeners (phone apps
and the Zello desktop client).

## Features

- **Native WebSocket transport** over OpenSSL (no libwebsockets dependency).
  Non-blocking POSIX sockets, RFC 6455 framing, client-side masking.
- **Opus encode/decode** at 16 kHz mono, 60 ms frames by default (matches the
  framing real Zello phone clients send on the wire).
- **Thread-safe API.** Every public function is safe to call from any thread
  (internally guarded by a recursive mutex). The typical pattern is
  `zello_client_send_pcm()` from an audio thread, `zello_client_poll()` from a
  service/main thread; libzello handles the concurrency.
- **Lock-free SPSC PCM ring** on the transmit path. The producer never blocks
  on the consumer; the consumer paces Opus encode + send at the configured
  frame cadence so listeners hear smooth real-time audio regardless of how
  fast the producer pushes samples in.
- **Single channel, full duplex.** One outbound stream and any number of
  inbound streams concurrently.
- **`refresh_token`-based reconnect** with exponential backoff (1 s → 60 s
  cap by default). On "no permission" (e.g. the server evicting us because a
  duplicate client logged in elsewhere), libzello discards the stale
  refresh_token and falls back to a fresh username+password logon on the
  next attempt.
- **JWT support** for Zello Friends & Family logons.
- **Logging callback** so the host can route libzello log lines into its own
  log infrastructure instead of stderr.

## Build

```sh
sudo apt install libssl-dev libopus-dev libcjson-dev pkg-config
./autogen.sh
./configure
make
sudo make install     # or build the .deb (see below)
```

### Debian package

```sh
dpkg-buildpackage -us -uc -b
sudo dpkg -i ../libzello0_*.deb ../libzello-dev_*.deb
```

## Quick API sketch

```c
#include <libzello/zello_client.h>

static void on_audio(zello_client_t *c, uint32_t sid,
                      const int16_t *pcm, size_t n,
                      int sr, void *ud)
{
    /* hand `pcm` (n samples at sr Hz, mono int16) to your playback path */
}

static void on_connected(zello_client_t *c, const char *rt, void *ud)
{
    /* channel joined; safe to call zello_client_start_tx() now */
}

zello_config_t cfg = {
    .server_url = "wss://zello.io/ws",
    .username   = "alice",
    .password   = "...",
    .channel    = "MyChannel",
    .auth_token = "<JWT from developers.zello.com>",
};
zello_callbacks_t cb = {
    .on_connected = on_connected,
    .on_audio     = on_audio,
    .userdata     = my_ctx,
};

zello_client_t *c = zello_client_create(&cfg, &cb);
zello_client_start(c);

for (;;) zello_client_poll(c, 100);   /* drives WS + reconnect + TX cadence */

/* transmit a buffer */
zello_client_start_tx(c);
zello_client_send_pcm(c, samples, n_samples);   /* any size */
zello_client_stop_tx(c);
```

`zello_client_send_pcm()` is lock-free; you can call it from a real-time
audio thread while `zello_client_poll()` is running on another thread.

## Examples

The `examples/` directory contains:

- **zello_listen** — connect listen-only, decode incoming streams, print
  speaker + decoded PCM info.
- **zello_tone** — connect, transmit a sine wave for N seconds.
- **zello_cli** — interactive REPL (linenoise-based). Commands include
  `status`, `connect`, `disconnect`, `say <text>`, `tone [hz] [ms]`,
  `wav <path>` (transmit a WAV file). Async log lines are queued and the
  prompt is redrawn between them so the UI stays usable while audio flows.

Most examples accept credentials via environment variables:

```sh
export ZELLO_USERNAME=alice
export ZELLO_PASSWORD=...
export ZELLO_CHANNEL=MyChannel
export ZELLO_AUTH_TOKEN=$(cat ~/devtoken.txt)
examples/zello_cli
```

## Tests

`tests/test_codec_roundtrip` loads a 16-bit mono WAV, encodes through
libzello's Opus + Zello binary packet wrapping, unwraps + decodes, and
writes the reconstructed WAV to `/tmp/test_roundtrip_out.wav`. Useful for
verifying the codec/protocol path independently of WS or server behaviour
when diagnosing audio degradation.

```sh
make check                                   # builds the test program
tests/test_codec_roundtrip path/to/input.wav
```

## Protocol notes

- TX framing matches what real Zello phone clients send on the wire:
  16 kHz, 1 frame per packet, 60 ms per frame. `codec_header` for the
  default config base64-encodes to `gD4BPA==`.
- Outbound `packet_id` is set to a per-stream sequence starting at 1, not
  to zero. The published API doc says the field is ignored on outbound,
  but the official JS SDK fills a sequence and the server-side path
  appears to use it for re-timing/de-duplication.
- The Opus encoder is reset (`OPUS_RESET_STATE`) at the start of every new
  outbound stream so the listener-side decoder, which starts fresh per
  `stream_id`, doesn't see carry-over prediction state from a prior
  stream.

## License

MIT — see [LICENSE](LICENSE).
