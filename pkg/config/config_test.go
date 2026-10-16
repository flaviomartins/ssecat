package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultFileConfigDisablesResume(t *testing.T) {
	t.Parallel()

	cfg := DefaultFileConfig()
	if cfg.Resume {
		t.Fatal("DefaultFileConfig().Resume = true, want false")
	}
	if !cfg.HTTP3 {
		t.Fatal("DefaultFileConfig().HTTP3 = false, want true")
	}
}

func TestLoadParsesContinueOverride(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, ".ssecatrc")
	if err := os.WriteFile(path, []byte("continue=true\n"), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Resume {
		t.Fatal("Load().Resume = false, want true")
	}
}

func TestLoadParsesHTTP3Preference(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, ".ssecatrc")
	if err := os.WriteFile(path, []byte("http3=true\n"), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.HTTP3 {
		t.Fatal("Load().HTTP3 = false, want true")
	}
}

func TestLoadCanDisableHTTP3Preference(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, ".ssecatrc")
	if err := os.WriteFile(path, []byte("http3=false\n"), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP3 {
		t.Fatal("Load().HTTP3 = true, want false")
	}
}

func TestLoadRejectsLegacyResumeKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, ".ssecatrc")
	if err := os.WriteFile(path, []byte("resume=true\n"), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want unknown key error")
	}
	if got, want := err.Error(), `unknown config key "resume"`; !strings.Contains(got, want) {
		t.Fatalf("Load() error = %q, want to contain %q", got, want)
	}
}

func TestIdleTimeoutConfig(t *testing.T) {
	t.Parallel()
	if got := DefaultFileConfig().IdleTimeout; got != 60*time.Second {
		t.Fatalf("default = %v, want 60s", got)
	}
	for in, want := range map[string]time.Duration{"idle-timeout=15s\n": 15 * time.Second, "idle-timeout=0\n": 0} {
		path := filepath.Join(t.TempDir(), ".ssecatrc")
		if err := os.WriteFile(path, []byte(in), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if err != nil || cfg.IdleTimeout != want {
			t.Fatalf("Load(%q) = %v, %v; want %v", in, cfg.IdleTimeout, err, want)
		}
	}
	for _, in := range []string{"idle-timeout=-1s\n", "idle-timeout=abc\n"} {
		path := filepath.Join(t.TempDir(), ".ssecatrc")
		_ = os.WriteFile(path, []byte(in), 0o600)
		if _, err := Load(path); err == nil {
			t.Fatalf("Load(%q) succeeded, want error", in)
		}
	}
}
