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
	"strings"
	"time"

	"github.com/flaviomartins/ssecat/pkg/output"
	"github.com/flaviomartins/ssecat/pkg/sse"
	"github.com/flaviomartins/ssecat/pkg/state"
	"github.com/quic-go/quic-go/http3"
)

const maxBackoff = 30 * time.Second

// Options configures a Client.
type Options struct {
	URL          string
	HTTPClient   *http.Client
	Headers      http.Header
	Writer       output.Writer
	StatusWriter io.Writer
	State        *state.Store
	Retry        bool
	InitialRetry time.Duration
	PreferHTTP3  bool
}

// Client receives and emits SSE streams.
type Client struct {
	url        string
	httpClient *http.Client
	headers    http.Header
	writer     output.Writer
	status     io.Writer
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
	if opts.PreferHTTP3 {
		httpClient := *opts.HTTPClient
		fallback := httpClient.Transport
		if fallback == nil {
			fallback = http.DefaultTransport
		}
		http3Transport := &http3.Transport{}
		if standardTransport, ok := fallback.(*http.Transport); ok && standardTransport.TLSClientConfig != nil {
			http3Transport.TLSClientConfig = standardTransport.TLSClientConfig.Clone()
		}
		httpClient.Transport = &http3FallbackTransport{
			http3:    http3Transport,
			fallback: fallback,
		}
		opts.HTTPClient = &httpClient
	}
	return &Client{
		url:        opts.URL,
		httpClient: opts.HTTPClient,
		headers:    h,
		writer:     opts.Writer,
		status:     opts.StatusWriter,
		state:      opts.State,
		retry:      opts.Retry,
		retryDelay: opts.InitialRetry,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type http3FallbackTransport struct {
	http3    http.RoundTripper
	fallback http.RoundTripper
}

func (t *http3FallbackTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.canUseHTTP3(req) {
		return t.fallback.RoundTrip(req)
	}

	http3Req := req.Clone(req.Context())
	if req.Body != nil && req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("recreate request body for HTTP/3: %w", err)
		}
		http3Req.Body = body
	}
	resp, http3Err := t.http3.RoundTrip(http3Req)
	if http3Err == nil || req.Context().Err() != nil {
		return resp, http3Err
	}
	if req.Body != nil && req.GetBody == nil {
		return nil, http3Err
	}

	fallbackReq := req.Clone(req.Context())
	if req.Body != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("recreate request body for HTTP fallback: %w", err)
		}
		fallbackReq.Body = body
	}
	fallbackResp, fallbackErr := t.fallback.RoundTrip(fallbackReq)
	if fallbackErr != nil {
		return nil, errors.Join(
			fmt.Errorf("HTTP/3 request: %w", http3Err),
			fmt.Errorf("HTTP fallback request: %w", fallbackErr),
		)
	}
	return fallbackResp, nil
}

func (t *http3FallbackTransport) canUseHTTP3(req *http.Request) bool {
	if req.URL.Scheme != "https" {
		return false
	}
	standardTransport, ok := t.fallback.(*http.Transport)
	if !ok || standardTransport.Proxy == nil {
		return true
	}
	proxyURL, err := standardTransport.Proxy(req)
	return err == nil && proxyURL == nil
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
			statusErr, isHTTPStatusError := errors.AsType[*httpStatusError](err)
			if !c.retry || (isHTTPStatusError && !statusErr.retryable()) {
				return err
			}
			delay := c.backoffDuration(attempt)
			if isHTTPStatusError && statusErr.code == http.StatusTooManyRequests {
				if statusErr.retryAfterSet {
					delay = statusErr.retryAfter
				}
				if c.status != nil {
					if _, writeErr := fmt.Fprintf(c.status, "ssecat: rate limited (HTTP 429); retrying in %s\n", delay); writeErr != nil {
						return fmt.Errorf("write rate-limit notice: %w", writeErr)
					}
				}
			}
			if err := c.wait(ctx, delay); err != nil {
				return nil
			}
			attempt++
			continue
		}

		attempt = 0
		done := make(chan struct{})
		go func(body io.Closer) {
			select {
			case <-ctx.Done():
				_ = body.Close()
			case <-done:
			}
		}(resp.Body)
		streamErr, updatedID := c.consume(ctx, resp.Body, &lastEventID)
		close(done)
		_ = resp.Body.Close()
		lastEventID = updatedID
		if streamErr != nil && !errors.Is(streamErr, io.EOF) {
			if errors.Is(streamErr, context.Canceled) {
				return nil
			}
			if !c.retry {
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
		retryAfter, retryAfterSet := time.Duration(0), false
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter, retryAfterSet = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		}
		return nil, &httpStatusError{
			code:          resp.StatusCode,
			status:        resp.Status,
			retryAfter:    retryAfter,
			retryAfterSet: retryAfterSet,
		}
	}
	return resp, nil
}

type httpStatusError struct {
	code          int
	status        string
	retryAfter    time.Duration
	retryAfterSet bool
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("unexpected HTTP status: %s", e.status)
}

func (e *httpStatusError) retryable() bool {
	return e.code == http.StatusRequestTimeout ||
		e.code == http.StatusTooManyRequests ||
		e.code >= 500
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	delaySeconds := true
	for _, r := range value {
		if r < '0' || r > '9' {
			delaySeconds = false
			break
		}
	}
	if delaySeconds {
		delay, err := time.ParseDuration(value + "s")
		if err != nil {
			return 0, false
		}
		return delay, true
	}
	retryAt, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := retryAt.Sub(now)
	if delay < 0 {
		return 0, true
	}
	return delay, true
}

func (c *Client) consume(ctx context.Context, body io.Reader, currentID *string) (error, string) {
	parser := sse.NewParser(body)
	lastID := *currentID
	for {
		ev, err := parser.Next()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err(), lastID
			}
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
	if netErr, ok := errors.AsType[net.Error](err); ok {
		return netErr.Timeout()
	}
	return false
}

func (c *Client) backoffDuration(attempt int) time.Duration {
	exponent := math.Pow(2, float64(attempt))
	d := min(time.Duration(float64(c.retryDelay)*exponent), maxBackoff)
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
