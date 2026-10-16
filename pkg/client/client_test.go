package client

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flaviomartins/ssecat/pkg/output"
	"github.com/klauspost/compress/zstd"
)

func TestHTTP3FallbackTransportFallsBackOnRequestError(t *testing.T) {
	t.Parallel()

	http3Err := errors.New("HTTP/3 unavailable")
	http3Calls, fallbackCalls := 0, 0
	transport := &http3FallbackTransport{
		http3: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			http3Calls++
			return nil, http3Err
		}),
		fallback: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			fallbackCalls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		}),
	}
	req, err := http.NewRequest(http.MethodGet, "https://example.test/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	_ = resp.Body.Close()
	if http3Calls != 1 || fallbackCalls != 1 {
		t.Fatalf("transport calls: HTTP/3 = %d, fallback = %d; want 1 each", http3Calls, fallbackCalls)
	}
}

func TestHTTP3FallbackTransportDoesNotFallbackOnHTTPStatus(t *testing.T) {
	t.Parallel()

	fallbackCalls := 0
	transport := &http3FallbackTransport{
		http3: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		}),
		fallback: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			fallbackCalls++
			return nil, errors.New("unexpected fallback")
		}),
	}
	req, err := http.NewRequest(http.MethodGet, "https://example.test/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	_ = resp.Body.Close()
	if fallbackCalls != 0 {
		t.Fatalf("fallback called %d times, want 0", fallbackCalls)
	}
}

func TestHTTP3FallbackTransportSkipsHTTP3ForHTTPURL(t *testing.T) {
	t.Parallel()

	http3Calls, fallbackCalls := 0, 0
	transport := &http3FallbackTransport{
		http3: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			http3Calls++
			return nil, errors.New("unexpected HTTP/3 request")
		}),
		fallback: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			fallbackCalls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		}),
	}
	req, err := http.NewRequest(http.MethodGet, "http://example.test/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	_ = resp.Body.Close()
	if http3Calls != 0 || fallbackCalls != 1 {
		t.Fatalf("transport calls: HTTP/3 = %d, fallback = %d; want 0 and 1", http3Calls, fallbackCalls)
	}
}

func TestHTTP3FallbackTransportSkipsHTTP3WhenProxyIsConfigured(t *testing.T) {
	t.Parallel()

	proxyURL := &url.URL{Scheme: "http", Host: "proxy.example.test"}
	transport := &http3FallbackTransport{
		http3: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("unexpected HTTP/3 request")
		}),
		fallback: &http.Transport{
			Proxy: func(*http.Request) (*url.URL, error) {
				return proxyURL, nil
			},
		},
	}
	req, err := http.NewRequest(http.MethodGet, "https://example.test/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	if transport.canUseHTTP3(req) {
		t.Fatal("canUseHTTP3() = true, want false")
	}
}

func TestRunExitsOnContextCancelWhileStreaming(t *testing.T) {
	t.Parallel()

	body := &blockingReadCloser{}
	c := New(Options{
		URL: "https://example.test/stream",
		HTTPClient: &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       body,
				}, nil
			}),
		},
		Writer: output.NewPayloadWriter(io.Discard),
		Retry:  false,
	})

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- c.Run(ctx)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not exit after context cancellation")
	}
}

func TestRunDoesNotRetryNotFound(t *testing.T) {
	t.Parallel()

	requests := 0
	c := New(Options{
		URL: "https://example.test/stream",
		HTTPClient: &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Status:     "404 Not Found",
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			}),
		},
		Writer:       output.NewPayloadWriter(io.Discard),
		Retry:        true,
		InitialRetry: time.Millisecond,
	})

	err := c.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "404 Not Found") {
		t.Fatalf("Run() error = %v, want 404 Not Found", err)
	}
	if requests != 1 {
		t.Fatalf("request count = %d, want 1", requests)
	}
}

func TestRunUsesRetryAfterForRateLimit(t *testing.T) {
	t.Parallel()

	requests := 0
	statusNotices := make(chan string, 1)
	c := New(Options{
		URL: "https://example.test/stream",
		HTTPClient: &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				requests++
				return &http.Response{
					StatusCode: http.StatusTooManyRequests,
					Status:     "429 Too Many Requests",
					Header:     http.Header{"Retry-After": []string{"60"}},
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    req,
				}, nil
			}),
		},
		Writer:       output.NewPayloadWriter(io.Discard),
		StatusWriter: statusNoticeWriter{notices: statusNotices},
		Retry:        true,
		InitialRetry: time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- c.Run(ctx)
	}()

	select {
	case notice := <-statusNotices:
		if notice != "ssecat: rate limited (HTTP 429); retrying in 1m0s\n" {
			t.Fatalf("rate-limit notice = %q", notice)
		}
	case <-time.After(time.Second):
		t.Fatal("no rate-limit notice was written")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run() error = %v, want nil after cancellation", err)
	}
	if requests != 1 {
		t.Fatalf("request count = %d, want 1 before Retry-After elapses", requests)
	}
}

func TestConsumeReturnsContextCanceled(t *testing.T) {
	t.Parallel()

	body := &blockingReadCloser{}
	ctx, cancel := context.WithCancel(context.Background())

	result := make(chan error, 1)
	go func() {
		err, _ := (&Client{}).consume(ctx, body, new(string))
		result <- err
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()
	_ = body.Close()

	select {
	case err := <-result:
		if err != context.Canceled {
			t.Fatalf("consume() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("consume() did not exit after context cancellation")
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)
	retryAt := now.Add(30 * time.Second).Format(http.TimeFormat)
	tests := []struct {
		name      string
		value     string
		want      time.Duration
		wantValid bool
	}{
		{name: "seconds", value: "45", want: 45 * time.Second, wantValid: true},
		{name: "http date", value: retryAt, want: 30 * time.Second, wantValid: true},
		{name: "past date", value: now.Add(-time.Second).Format(http.TimeFormat), want: 0, wantValid: true},
		{name: "invalid", value: "later", wantValid: false},
		{name: "negative seconds", value: "-1", wantValid: false},
		{name: "fractional seconds", value: "1.5", wantValid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, valid := parseRetryAfter(tt.value, now)
			if got != tt.want || valid != tt.wantValid {
				t.Errorf("parseRetryAfter(%q) = (%s, %t), want (%s, %t)", tt.value, got, valid, tt.want, tt.wantValid)
			}
		})
	}
}

func TestHTTPStatusErrorRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code      int
		retryable bool
	}{
		{code: http.StatusNotFound, retryable: false},
		{code: http.StatusRequestTimeout, retryable: true},
		{code: http.StatusTooManyRequests, retryable: true},
		{code: http.StatusServiceUnavailable, retryable: true},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.code), func(t *testing.T) {
			err := &httpStatusError{code: tt.code}
			if got := err.retryable(); got != tt.retryable {
				t.Errorf("retryable() = %t, want %t", got, tt.retryable)
			}
		})
	}
}

type statusNoticeWriter struct {
	notices chan<- string
}

func (w statusNoticeWriter) Write(p []byte) (int, error) {
	w.notices <- string(p)
	return len(p), nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type blockingReadCloser struct {
	mu     sync.Mutex
	closed bool
	wait   chan struct{}
}

func (b *blockingReadCloser) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.wait == nil {
		b.wait = make(chan struct{})
	}
	wait := b.wait
	closed := b.closed
	b.mu.Unlock()

	if closed {
		return 0, io.EOF
	}

	<-wait
	return 0, io.EOF
}

func (b *blockingReadCloser) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	if b.wait == nil {
		b.wait = make(chan struct{})
	}
	close(b.wait)
	return nil
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestNewHTTPClientHasNoWholeRequestTimeout(t *testing.T) {
	t.Parallel()
	c := NewHTTPClient()
	if c.Timeout != 0 {
		t.Fatalf("Timeout = %v, want 0", c.Timeout)
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok || tr.ResponseHeaderTimeout != DefaultResponseHeaderTimeout {
		t.Fatalf("transport = %#v, want ResponseHeaderTimeout %v", c.Transport, DefaultResponseHeaderTimeout)
	}
}

func TestRunDecodesCompressedStreams(t *testing.T) {
	t.Parallel()

	for _, encoding := range []string{"zstd", "gzip"} {
		t.Run(encoding, func(t *testing.T) {
			var compressed bytes.Buffer
			var writer io.WriteCloser
			if encoding == "zstd" {
				var err error
				writer, err = zstd.NewWriter(&compressed)
				if err != nil {
					t.Fatalf("create compressor: %v", err)
				}
			} else {
				writer = gzip.NewWriter(&compressed)
			}
			if _, err := io.WriteString(writer, "data: compressed event\n\n"); err != nil {
				t.Fatalf("write compressed event: %v", err)
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("close compressor: %v", err)
			}

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Accept-Encoding"); got != "gzip, zstd" {
					t.Errorf("Accept-Encoding = %q, want %q", got, "gzip, zstd")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Content-Encoding", encoding)
				_, _ = w.Write(compressed.Bytes())
			}))
			defer srv.Close()

			out := &syncBuf{}
			c := New(Options{URL: srv.URL, Writer: output.NewPayloadWriter(out)})
			if err := c.Run(context.Background()); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if got := out.String(); got != "compressed event\n" {
				t.Fatalf("output = %q, want %q", got, "compressed event\n")
			}
		})
	}
}

func TestLongStreamWithKeepaliveSurvivesIdleTimeout(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for i := range 8 {
			if i%2 == 0 {
				fmt.Fprintf(w, "data: e%d\n\n", i)
			} else {
				fmt.Fprint(w, ": keepalive\n\n")
			}
			fl.Flush()
			time.Sleep(100 * time.Millisecond)
		}
	}))
	defer srv.Close()

	out := &syncBuf{}
	c := New(Options{URL: srv.URL, Writer: output.NewPayloadWriter(out), IdleTimeout: 300 * time.Millisecond})
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := out.String(); got != "e0\ne2\ne4\ne6\n" {
		t.Fatalf("output = %q", got)
	}
}

func idleServer(conns *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conns.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: hello\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
}

func TestIdleTimeoutReconnects(t *testing.T) {
	t.Parallel()
	var conns atomic.Int32
	srv := idleServer(&conns)
	defer srv.Close()

	out := &syncBuf{}
	c := New(Options{
		URL: srv.URL, Writer: output.NewPayloadWriter(out), Retry: true,
		InitialRetry: 10 * time.Millisecond, IdleTimeout: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if conns.Load() < 2 || strings.Count(out.String(), "hello") < 2 {
		t.Fatalf("conns = %d, output = %q; want reconnects", conns.Load(), out.String())
	}
}

func TestIdleTimeoutErrorsWhenRetryDisabled(t *testing.T) {
	t.Parallel()
	var conns atomic.Int32
	srv := idleServer(&conns)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Writer: output.NewPayloadWriter(io.Discard), IdleTimeout: 100 * time.Millisecond})
	err := c.Run(context.Background())
	if !errors.Is(err, ErrIdleTimeout) {
		t.Fatalf("Run() error = %v, want ErrIdleTimeout", err)
	}
}

func TestZeroIdleTimeoutDisables(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: a\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(400 * time.Millisecond)
		fmt.Fprint(w, "data: b\n\n")
	}))
	defer srv.Close()

	out := &syncBuf{}
	c := New(Options{URL: srv.URL, Writer: output.NewPayloadWriter(out)})
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := out.String(); got != "a\nb\n" {
		t.Fatalf("output = %q", got)
	}
}

// ackWriter records each payload as ssecat writes it and releases the server
// to send the next event.
type ackWriter struct {
	got chan string
	ack chan<- struct{}
}

func (w ackWriter) Write(p []byte) (int, error) {
	w.got <- string(p)
	w.ack <- struct{}{}
	return len(p), nil
}

type flushWriteCloser interface {
	io.WriteCloser
	Flush() error
}

func newFlushCompressor(t *testing.T, encoding string, w io.Writer) flushWriteCloser {
	t.Helper()
	switch encoding {
	case "gzip":
		return gzip.NewWriter(w)
	case "zstd":
		zw, err := zstd.NewWriter(w)
		if err != nil {
			t.Errorf("create zstd writer: %v", err)
			return nil
		}
		return zw
	}
	t.Errorf("unsupported encoding %q", encoding)
	return nil
}

// assertIncremental streams events through opts.URL, requiring each one to be
// emitted before the server sends the next, so buffered decoding would time out.
func assertIncremental(t *testing.T, opts Options, events int, ack chan<- struct{}) {
	t.Helper()
	got := make(chan string, events)
	opts.Writer = output.NewPayloadWriter(ackWriter{got: got, ack: ack})
	c := New(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	for i := range events {
		select {
		case line := <-got:
			if want := fmt.Sprintf("event-%d\n", i); line != want {
				t.Fatalf("event %d = %q, want %q", i, line, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d was not emitted before the next was sent", i)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestRunEmitsCompressedEventsIncrementally(t *testing.T) {
	t.Parallel()

	for _, encoding := range []string{"gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			t.Parallel()
			const events = 3
			ack := make(chan struct{}, events)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Content-Encoding", encoding)
				cw := newFlushCompressor(t, encoding, w)
				if cw == nil {
					return
				}
				for i := range events {
					fmt.Fprintf(cw, "data: event-%d\n\n", i)
					_ = cw.Flush()
					w.(http.Flusher).Flush()
					select {
					case <-ack:
					case <-time.After(5 * time.Second):
						return
					}
				}
				_ = cw.Close()
			}))
			defer srv.Close()

			assertIncremental(t, Options{URL: srv.URL}, events, ack)
		})
	}
}
