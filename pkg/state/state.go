package state

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Store persists Last-Event-ID values.
type Store struct {
	baseDir string
}

// New creates a state store using stateDir or platform default.
func New(stateDir string) (*Store, error) {
	if stateDir == "" {
		d, err := defaultDir()
		if err != nil {
			return nil, err
		}
		stateDir = d
	}
	return &Store{baseDir: filepath.Clean(stateDir)}, nil
}

// Load reads a previously stored Last-Event-ID for rawURL.
func (s *Store) Load(rawURL string) (string, error) {
	path, err := s.pathForURL(rawURL)
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("read state %q: %w", path, err)
	}
	return string(b), nil
}

// Save stores Last-Event-ID for rawURL using atomic replace.
func (s *Store) Save(rawURL string, id string) error {
	path, err := s.pathForURL(rawURL)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state directory %q: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".last-event-id-*")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.WriteString(id); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp state file %q: %w", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp state file %q: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state file %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp state file to %q: %w", path, err)
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}

func defaultDir() (string, error) {
	if runtime.GOOS == "darwin" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		return filepath.Join(home, "Library", "Application Support", "ssecat"), nil
	}
	d, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(d, "ssecat"), nil
}

func (s *Store) pathForURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse URL %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("URL %q has empty host", rawURL)
	}

	host := sanitizeComponent(u.Host)
	segments := splitPath(u.Path)
	for i := range segments {
		segments[i] = sanitizeComponent(segments[i])
	}

	parts := []string{s.baseDir, host}
	if len(segments) == 0 {
		parts = append(parts, "_root.last-event-id")
	} else {
		parts = append(parts, segments[:len(segments)-1]...)
		parts = append(parts, segments[len(segments)-1]+".last-event-id")
	}
	fullPath := filepath.Join(parts...)
	cleanBase := filepath.Clean(s.baseDir)
	cleanPath := filepath.Clean(fullPath)
	rel, err := filepath.Rel(cleanBase, cleanPath)
	if err != nil {
		return "", fmt.Errorf("compute relative path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("resolved path escapes state directory: %q", cleanPath)
	}
	return cleanPath, nil
}

func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	items := strings.Split(trimmed, "/")
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}

func sanitizeComponent(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return "_"
	}
	var b strings.Builder
	for _, r := range input {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	name := b.String()
	if name == "" || name == "." || name == ".." {
		return "_"
	}
	return name
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open state directory %q: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync state directory %q: %w", dir, err)
	}
	return nil
}
