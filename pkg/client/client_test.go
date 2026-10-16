package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flaviomartins/ssecat/pkg/output"
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
