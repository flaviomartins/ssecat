package sse

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Event is a parsed Server-Sent Event.
type Event struct {
	ID    string
	Event string
	Data  string
	Retry time.Duration
}

// Parser reads Server-Sent Events from a stream.
type Parser struct {
	r *bufio.Reader

	data         []string
	eventType    string
	lastEventID  string
	currentRetry time.Duration
	retrySet     bool
	eof          bool
}

// NewParser creates a Parser reading from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{r: bufio.NewReader(r)}
}

// Next returns the next parsed event.
func (p *Parser) Next() (Event, error) {
	for {
		line, sawEOF, err := p.readLine()
		if err != nil {
			return Event{}, err
		}

		if sawEOF {
			p.eof = true
		}

		if line == "" {
			ev, ok := p.dispatch()
			if ok {
				return ev, nil
			}
			if p.eof {
				return Event{}, io.EOF
			}
			continue
		}

		if strings.HasPrefix(line, ":") {
			if p.eof {
				ev, ok := p.dispatch()
				if ok {
					return ev, nil
				}
				return Event{}, io.EOF
			}
			continue
		}

		field := line
		value := ""
		if k, v, ok := strings.Cut(line, ":"); ok {
			field = k
			value = v
			if strings.HasPrefix(value, " ") {
				value = value[1:]
			}
		}

		switch field {
		case "data":
			p.data = append(p.data, value)
		case "event":
			p.eventType = value
		case "id":
			if !strings.ContainsRune(value, '\x00') {
				p.lastEventID = value
			}
		case "retry":
			if retry, ok := parseRetry(value); ok {
				p.currentRetry = retry
				p.retrySet = true
			}
		}

		if p.eof {
			ev, ok := p.dispatch()
			if ok {
				return ev, nil
			}
			return Event{}, io.EOF
		}
	}
}

func (p *Parser) dispatch() (Event, bool) {
	if len(p.data) == 0 {
		p.eventType = ""
		p.currentRetry = 0
		p.retrySet = false
		return Event{}, false
	}
	eventType := p.eventType
	if eventType == "" {
		eventType = "message"
	}
	ev := Event{
		ID:    p.lastEventID,
		Event: eventType,
		Data:  strings.Join(p.data, "\n"),
	}
	if p.retrySet {
		ev.Retry = p.currentRetry
	}
	p.data = nil
	p.eventType = ""
	p.currentRetry = 0
	p.retrySet = false
	return ev, true
}

func parseRetry(v string) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	for _, r := range v {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return time.Duration(n) * time.Millisecond, true
}

func (p *Parser) readLine() (string, bool, error) {
	if p.eof {
		return "", true, io.EOF
	}
	var b bytes.Buffer
	for {
		ch, err := p.r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if b.Len() == 0 {
					return "", true, nil
				}
				return b.String(), true, nil
			}
			return "", false, fmt.Errorf("read byte: %w", err)
		}
		switch ch {
		case '\n':
			return b.String(), false, nil
		case '\r':
			next, err := p.r.Peek(1)
			if err == nil && len(next) == 1 && next[0] == '\n' {
				_, _ = p.r.ReadByte()
			}
			return b.String(), false, nil
		default:
			_ = b.WriteByte(ch)
		}
	}
}
