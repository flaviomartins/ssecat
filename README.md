# ssecat

`ssecat` is a command-line tool for Server-Sent Events (SSE), similar to how `curl` works for regular HTTP responses.

## Overview

- Prefers HTTP/3 for HTTPS streams, falling back to HTTP/1.1 or HTTP/2 using Go's standard `net/http` stack.
- Requests gzip and Zstandard compression by default and decodes compressed streams.
- Parses SSE according to the WHATWG EventSource format.
- Writes only event payload (`data`) to stdout by default.
- Writes diagnostics and errors to stderr.
- Supports automatic reconnect, with optional Last-Event-ID resume.
- Exits cleanly on `Ctrl-C` across macOS, Linux, and Windows.

## Installation

### From source

```bash
go install github.com/flaviomartins/ssecat/cmd/ssecat@latest
```

Building from source requires Go 1.27.0 or newer.

### From release artifacts

Download binaries from GitHub Releases and place `ssecat` in your `PATH`.
Release binaries are built with the latest stable Go toolchain.

## Building

```bash
make build
```

or:

```bash
go build -o ssecat ./cmd/ssecat
```

## Usage

```bash
ssecat [flags] URL
```

Flags:

- `--config`
- `--state-dir`
- `--continue`, `-c`
- `--header` (repeatable, format: `Name: Value`)
- `--http3` (enable HTTP/3 preference)
- `--no-http3` (disable HTTP/3 preference)
- `--idle-timeout` (Go duration; reconnect if no bytes arrive for this long; `0` disables; default `60s`)
- `--version`
- `--help`

## Examples

### Basic stream

```bash
ssecat https://stream.wikimedia.org/v2/stream/recentchange
```

### Pipe into jq

```bash
ssecat https://stream.wikimedia.org/v2/stream/recentchange | jq .
```

### Pipe into grep

```bash
ssecat https://stream.wikimedia.org/v2/stream/recentchange | grep Wikipedia
```

### Pipe into awk

```bash
ssecat https://stream.wikimedia.org/v2/stream/recentchange | awk 'NR<=10 {print}'
```

## Configuration

Single config file:

`~/.config/ssecat/.ssecatrc`

Supported keys:

```ini
retry=true
retry-delay=2s
continue=false
user-agent=ssecat/0.1
accept=text/event-stream
http3=true
idle-timeout=60s
```

## State directory

`ssecat` stores Last-Event-ID in the user state directory:

- Linux: `~/.local/state/ssecat`
- macOS: `~/Library/Application Support/ssecat`
- Windows: system user state/config directory from Go

URL path mapping is direct (sanitized), for example:

`https://stream.wikimedia.org/v2/stream/recentchange`

becomes:

```text
stream.wikimedia.org/
  v2/
    stream/
      recentchange.last-event-id
```

## Last-Event-ID and reconnect behavior

- Initial retry delay is 3 seconds by default.
- `retry:` fields from the server update reconnect delay.
- Transport failures and retryable HTTP statuses use exponential backoff with jitter and cap at 30 seconds. Permanent 4xx responses (including 404) fail immediately; 408 and 429 are retried. For 429 responses, `Retry-After` is honored when present (delay-seconds or HTTP-date), and the wait is reported to stderr.
- Successful reconnect resets transport backoff.
- Reconnect requests include `Last-Event-ID` when available.
- There is no whole-request timeout. A stream that is receiving data is never cut.
- If no bytes arrive for the idle timeout (default 60s, `--idle-timeout` or `idle-timeout=` in config, `0` disables), the connection is dropped and, with retry enabled, reconnected. With retry disabled ssecat exits with an error. SSE comment lines such as `: keepalive` count as activity.
- A server that does not send response headers within 30s is treated as a connection failure.
- Resume is disabled by default; enable with `--continue`/`-c` or `continue=true` in config.

## HTTP/3

For `https://` URLs, `ssecat` tries HTTP/3 first by default, using the
[`quic-go`](https://github.com/quic-go/quic-go) transport. It falls back to the
standard HTTP/1.1 or HTTP/2 transport if the HTTP/3 request fails before a
response is received. Disable HTTP/3 with `--no-http3` or `http3=false` in the
config file; `--http3` enables it if the config disables it.
HTTP error status codes are not retried over another protocol. If an established
stream later disconnects, the existing reconnect behavior applies; an active
stream is not migrated between protocols.

HTTP/3 requires QUIC traffic to reach the server, usually over UDP port 443.
When UDP/QUIC is blocked or the server does not support HTTP/3, fallback may
add connection delay before the stream starts. If the configured HTTP transport
uses a proxy, HTTP/3 is skipped because the QUIC transport does not use that
proxy.

## Compression

By default `ssecat` sends `Accept-Encoding: gzip, zstd` and decodes `gzip` and `zstd` responses as they stream. Other encodings (for example `br` or `deflate`) are not supported and fail with an error. A header set with `--header 'Accept-Encoding: ...'` replaces the default; use `identity` to disable compression.

Compressed SSE is only incremental if the server flushes after each event. Decoding is tested against [Caddy](https://caddyserver.com/) using `encode gzip zstd` (see `pkg/client/caddy_test.go`, run in CI); other servers and proxies are not tested, and some, such as NGINX by default, do not compress `text/event-stream`.

## XDG directories

- Config: `~/.config/ssecat/.ssecatrc`
- State: `~/.local/state/ssecat` (or OS equivalent)

## Comparison with curl

`curl` can stream bytes but does not natively parse SSE fields. `ssecat` is purpose-built for EventSource streams and reconnect semantics.

## Development

```bash
make fmt
make vet
make test
make build
```

## Contributing

Contributions are welcome. Please run formatting, vet, and tests before opening pull requests.

## License

MIT (see `LICENSE`).
