package state

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestStoreSaveLoad(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	url := "https://stream.wikimedia.org/v2/stream/recentchange"
	if got, err := s.Load(url); err != nil || got != "" {
		t.Fatalf("initial Load() = %q, %v, want empty nil", got, err)
	}
	if err := s.Save(url, "123"); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	got, err := s.Load(url)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != "123" {
		t.Fatalf("Load() = %q, want 123", got)
	}
	if err := s.Save(url, ""); err != nil {
		t.Fatalf("Save(empty) error = %v", err)
	}
	got, err = s.Load(url)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != "" {
		t.Fatalf("Load() = %q, want empty", got)
	}
}

func TestPathForURLMapping(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	p, err := s.pathForURL("https://stream.wikimedia.org/v2/stream/recentchange")
	if err != nil {
		t.Fatalf("pathForURL() error = %v", err)
	}
	want := filepath.Join(s.baseDir, "stream.wikimedia.org", "v2", "stream", "recentchange.last-event-id")
	if p != want {
		t.Fatalf("pathForURL() = %q, want %q", p, want)
	}
}

func TestDefaultDir(t *testing.T) {
	d, err := defaultDir()
	if err != nil {
		t.Fatalf("defaultDir() error = %v", err)
	}
	if d == "" {
		t.Fatal("defaultDir() returned empty path")
	}
	if runtime.GOOS == "darwin" && filepath.Base(d) != "ssecat" {
		t.Fatalf("defaultDir() for darwin should end in ssecat, got %q", d)
	}
}

func TestSyncDir(t *testing.T) {
	if err := syncDir(t.TempDir()); err != nil {
		t.Fatalf("syncDir() error = %v", err)
	}
}

func TestSyncDirNonExistent(t *testing.T) {
	err := syncDir(filepath.Join(t.TempDir(), "missing"))
	if runtime.GOOS == "windows" {
		if err != nil {
			t.Fatalf("syncDir() on Windows error = %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("syncDir() expected error for non-existent directory, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("syncDir() error = %v, want os.ErrNotExist", err)
	}
}
