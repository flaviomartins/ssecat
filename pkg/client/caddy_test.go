package client

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestCaddyCompressedStreams runs ssecat through a real Caddy `encode` proxy.
// It is skipped when caddy is not installed unless SSECAT_REQUIRE_CADDY=1.
func TestCaddyCompressedStreams(t *testing.T) {
	caddyPath, err := exec.LookPath("caddy")
	if err != nil {
		if os.Getenv("SSECAT_REQUIRE_CADDY") == "1" {
			t.Fatal("caddy not found in PATH")
		}
		t.Skip("caddy not found in PATH")
	}

	const events = 3
	ack := make(chan struct{}, events)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		if r.Header.Get("X-Probe") != "" {
			fmt.Fprint(w, "data: probe\n\n")
			return
		}
		for i := range events {
			fmt.Fprintf(w, "data: event-%d\n\n", i)
			fl.Flush()
			select {
			case <-ack:
			case <-r.Context().Done():
				return
			case <-time.After(5 * time.Second):
				return
			}
		}
	}))
	defer backend.Close()

	addr := startCaddy(t, caddyPath, backend.Listener.Addr().String())

	for _, encoding := range []string{"gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			url := "http://" + addr
			if got := probeContentEncoding(t, url, encoding); got != encoding {
				t.Fatalf("Content-Encoding = %q, want %q", got, encoding)
			}
			assertIncremental(t, Options{
				URL:     url,
				Headers: http.Header{"Accept-Encoding": []string{encoding}},
			}, events, ack)
		})
	}
}

// probeContentEncoding confirms Caddy really compresses, so the test cannot pass on plain text.
func probeContentEncoding(t *testing.T, url, encoding string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Encoding", encoding)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Probe", "1")
	tr := &http.Transport{DisableCompression: true}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatalf("probe request: %v", err)
	}
	defer resp.Body.Close()
	return resp.Header.Get("Content-Encoding")
}

func startCaddy(t *testing.T, caddyPath, backendAddr string) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	cfg := filepath.Join(t.TempDir(), "Caddyfile")
	caddyfile := fmt.Sprintf(`{
	admin off
	auto_https off
}
http://%s {
	encode gzip zstd
	reverse_proxy %s {
		flush_interval -1
	}
}
`, addr, backendAddr)
	if err := os.WriteFile(cfg, []byte(caddyfile), 0o600); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	cmd := exec.Command(caddyPath, "run", "--config", cfg, "--adapter", "caddyfile")
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start caddy: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("caddy did not start listening on %s; logs:\n%s", addr, logs.String())
	return ""
}
