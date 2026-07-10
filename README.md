# ssecat

`ssecat` is a command-line tool for Server-Sent Events (SSE), similar to how `curl` works for regular HTTP responses.

## Overview

- Streams SSE over HTTP/1.1 or HTTP/2 using Go's standard `net/http` stack.
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

Building from source requires Go 1.25.0 or newer.

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
- `--resume`
- `--header` (repeatable, format: `Name: Value`)
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
resume=false
user-agent=ssecat/0.1
accept=text/event-stream
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
- Transport and HTTP failures use exponential backoff with jitter and cap at 30 seconds.
- Successful reconnect resets transport backoff.
- Reconnect requests include `Last-Event-ID` when available.
- Resume is disabled by default; enable with `--resume` or `resume=true` in config.

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
