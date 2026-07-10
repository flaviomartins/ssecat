package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultFileConfigDisablesResume(t *testing.T) {
	t.Parallel()

	cfg := DefaultFileConfig()
	if cfg.Resume {
		t.Fatal("DefaultFileConfig().Resume = true, want false")
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
