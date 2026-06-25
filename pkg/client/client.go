package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/flaviomartins/ssecat/pkg/output"
	"github.com/flaviomartins/ssecat/pkg/sse"
	"github.com/flaviomartins/ssecat/pkg/state"
)

const maxBackoff = 30 * time.Second

// Options configures a Client.
type Options struct {
	URL          string
	HTTPClient   *http.Client
	Headers      http.Header
	Writer       output.Writer
	State        *state.Store
	Retry        bool
	InitialRetry time.Duration
}

// Client receives and emits SSE streams.
type Client struct {
	url        string
	httpClient *http.Client
	headers    http.Header
	writer     output.Writer
	state      *state.Store
	retry      bool
	retryDelay time.Duration
	rand       *rand.Rand
}

// New constructs a Client.
func New(opts Options) *Client {
	h := make(http.Header, len(opts.Headers))
	for k, v := range opts.Headers {
		cp := make([]string, len(v))
		copy(cp, v)
		h[k] = cp
	}
	if opts.InitialRetry <= 0 {
		opts.InitialRetry = 3 * time.Second
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &Client{
		url:        opts.URL,
		httpClient: opts.HTTPClient,
		headers:    h,
		writer:     opts.Writer,
		state:      opts.State,
		retry:      opts.Retry,
		retryDelay: opts.InitialRetry,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run starts the stream receive loop.
func (c *Client) Run(ctx context.Context) error {
	if c.writer == nil {
		return errors.New("writer is required")
	}
	if c.url == "" {
		return errors.New("URL is required")
	}

	lastEventID := ""
	if c.state != nil {
		storedID, err := c.state.Load(c.url)
		if err != nil {
			return fmt.Errorf("load state: %w", err)
		}
		lastEventID = storedID
	}

	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		resp, err := c.connect(ctx, lastEventID)
		if err != nil {
			if !c.retry {
				return err
			}
			if err := c.wait(ctx, c.backoffDuration(attempt)); err != nil {
				return nil
			}
			attempt++
			continue
		}

		attempt = 0
		streamErr, updatedID := c.consume(resp.Body, &lastEventID)
		resp.Body.Close()
		lastEventID = updatedID
		if streamErr != nil && !errors.Is(streamErr, io.EOF) {
			if !c.retry || errors.Is(streamErr, context.Canceled) {
				return streamErr
			}
		}
		if !c.retry {
			if errors.Is(streamErr, io.EOF) || streamErr == nil {
				return nil
			}
			return streamErr
		}
		if err := c.wait(ctx, c.jitter(c.retryDelay)); err != nil {
			return nil
		}
	}
}

func (c *Client) connect(ctx context.Context, lastEventID string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	for k, values := range c.headers {
		for _, value := range values {
			req.Header.Add(k, value)
		}
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "text/event-stream")
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request stream: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
	}
	return resp, nil
}

func (c *Client) consume(body io.Reader, currentID *string) (error, string) {
	parser := sse.NewParser(body)
	lastID := *currentID
	for {
		ev, err := parser.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF, lastID
			}
			if isTemporary(err) {
				return err, lastID
			}
			return err, lastID
		}
		if ev.Retry > 0 {
			c.retryDelay = ev.Retry
		}
		if ev.ID != lastID {
			lastID = ev.ID
			if c.state != nil {
				if err := c.state.Save(c.url, lastID); err != nil {
					return fmt.Errorf("save state: %w", err), lastID
				}
			}
		}
		if err := c.writer.WriteEvent(ev); err != nil {
			return fmt.Errorf("write event: %w", err), lastID
		}
	}
}

func isTemporary(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

func (c *Client) backoffDuration(attempt int) time.Duration {
	exponent := math.Pow(2, float64(attempt))
	d := time.Duration(float64(c.retryDelay) * exponent)
	if d > maxBackoff {
		d = maxBackoff
	}
	return c.jitter(d)
}

func (c *Client) jitter(base time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	factor := 0.8 + (0.4 * c.rand.Float64())
	j := time.Duration(float64(base) * factor)
	if j > maxBackoff {
		return maxBackoff
	}
	if j < 0 {
		return 0
	}
	return j
}

func (c *Client) wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
