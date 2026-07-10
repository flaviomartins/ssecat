package client

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/flaviomartins/ssecat/pkg/output"
)

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
