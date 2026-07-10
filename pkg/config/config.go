package config

import (
	"bufio"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FileConfig stores supported .ssecatrc options.
type FileConfig struct {
	Retry      bool
	RetryDelay time.Duration
	Resume     bool
	UserAgent  string
	Accept     string
}

// DefaultPath returns the default configuration file path.
func DefaultPath() (string, error) {
	root, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(root, "ssecat", ".ssecatrc"), nil
}

// DefaultFileConfig returns defaults for configuration values.
func DefaultFileConfig() FileConfig {
	return FileConfig{
		Retry:      true,
		RetryDelay: 3 * time.Second,
		Resume:     false,
		UserAgent:  "ssecat/0.1",
		Accept:     "text/event-stream",
	}
}

// Load parses configuration from an INI-like key=value file.
func Load(path string) (FileConfig, error) {
	cfg := DefaultFileConfig()
	if path == "" {
		return cfg, nil
	}

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("open config file %q: %w", path, err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	lineNo := 0
	for s.Scan() {
		lineNo++
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return cfg, fmt.Errorf("invalid config line %d: expected key=value", lineNo)
		}
		key := strings.TrimSpace(strings.ToLower(k))
		value := strings.TrimSpace(v)
		switch key {
		case "retry":
			parsed, err := parseBool(value)
			if err != nil {
				return cfg, fmt.Errorf("parse retry on line %d: %w", lineNo, err)
			}
			cfg.Retry = parsed
		case "retry-delay":
			d, err := time.ParseDuration(value)
			if err != nil {
				return cfg, fmt.Errorf("parse retry-delay on line %d: %w", lineNo, err)
			}
			cfg.RetryDelay = d
		case "continue", "resume":
			parsed, err := parseBool(value)
			if err != nil {
				return cfg, fmt.Errorf("parse %s on line %d: %w", key, lineNo, err)
			}
			cfg.Resume = parsed
		case "user-agent":
			cfg.UserAgent = value
		case "accept":
			cfg.Accept = value
		default:
			return cfg, fmt.Errorf("unknown config key %q on line %d", key, lineNo)
		}
	}
	if err := s.Err(); err != nil {
		return cfg, fmt.Errorf("scan config file %q: %w", path, err)
	}
	return cfg, nil
}

// ValidateURL ensures the URL is usable for HTTP(S) streaming.
func ValidateURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported URL scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("URL host is required")
	}
	return u, nil
}

func parseBool(v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean %q", v)
	}
}
