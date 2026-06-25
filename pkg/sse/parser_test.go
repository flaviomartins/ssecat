package sse

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestParserNext_Table(t *testing.T) {
	largePayload := strings.Repeat("x", 10000)
	tests := []struct {
		name   string
		input  string
		events []Event
	}{
		{name: "single event", input: "data:hello\n\n", events: []Event{{Event: "message", Data: "hello"}}},
		{name: "multiple events", input: "data:a\n\ndata:b\n\n", events: []Event{{Event: "message", Data: "a"}, {Event: "message", Data: "b"}}},
		{name: "multiline data", input: "data:line1\ndata:line2\n\n", events: []Event{{Event: "message", Data: "line1\nline2"}}},
		{name: "comments", input: ": comment\ndata:ok\n\n", events: []Event{{Event: "message", Data: "ok"}}},
		{name: "retry field", input: "retry:1500\ndata:x\n\n", events: []Event{{Event: "message", Data: "x", Retry: 1500 * time.Millisecond}}},
		{name: "event field", input: "event:update\ndata:x\n\n", events: []Event{{Event: "update", Data: "x"}}},
		{name: "id field", input: "id:42\ndata:x\n\n", events: []Event{{ID: "42", Event: "message", Data: "x"}}},
		{name: "empty id", input: "id\ndata:x\n\n", events: []Event{{ID: "", Event: "message", Data: "x"}}},
		{name: "empty data", input: "data\n\n", events: []Event{{Event: "message", Data: ""}}},
		{name: "unknown field", input: "foo:bar\ndata:x\n\n", events: []Event{{Event: "message", Data: "x"}}},
		{name: "crlf", input: "data:x\r\n\r\n", events: []Event{{Event: "message", Data: "x"}}},
		{name: "cr", input: "data:x\r\r", events: []Event{{Event: "message", Data: "x"}}},
		{name: "lf", input: "data:x\n\n", events: []Event{{Event: "message", Data: "x"}}},
		{name: "eof without blank line", input: "data:x", events: []Event{{Event: "message", Data: "x"}}},
		{name: "invalid retry", input: "retry:abc\ndata:x\n\n", events: []Event{{Event: "message", Data: "x"}}},
		{name: "large payload", input: "data:" + largePayload + "\n\n", events: []Event{{Event: "message", Data: largePayload}}},
		{name: "utf8 payload", input: "data:Olá 世界 🌍\n\n", events: []Event{{Event: "message", Data: "Olá 世界 🌍"}}},
		{name: "blank lines", input: "\n\ndata:x\n\n", events: []Event{{Event: "message", Data: "x"}}},
		{name: "multiple blank lines", input: "\n\n\n\ndata:x\n\n", events: []Event{{Event: "message", Data: "x"}}},
		{name: "field without colon", input: "event\ndata:x\n\n", events: []Event{{Event: "message", Data: "x"}}},
		{name: "preserve empty data field", input: "data:\ndata:y\n\n", events: []Event{{Event: "message", Data: "\ny"}}},
		{name: "id persists across events", input: "id:10\ndata:a\n\ndata:b\n\n", events: []Event{{ID: "10", Event: "message", Data: "a"}, {ID: "10", Event: "message", Data: "b"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tc.input))
			var got []Event
			for {
				ev, err := p.Next()
				if err != nil {
					if err == io.EOF {
						break
					}
					t.Fatalf("Next() error = %v", err)
				}
				got = append(got, ev)
			}
			if len(got) != len(tc.events) {
				t.Fatalf("events length mismatch: got=%d want=%d", len(got), len(tc.events))
			}
			for i := range tc.events {
				if got[i] != tc.events[i] {
					t.Fatalf("event[%d] mismatch:\n got=%+v\nwant=%+v", i, got[i], tc.events[i])
				}
			}
		})
	}
}

func TestParserNext_EmptyStreamEOF(t *testing.T) {
	p := NewParser(strings.NewReader(""))
	_, err := p.Next()
	if err != io.EOF {
		t.Fatalf("Next() error = %v, want io.EOF", err)
	}
}

func TestParseRetry(t *testing.T) {
	tests := []struct {
		in   string
		ok   bool
		want time.Duration
	}{
		{in: "0", ok: true, want: 0},
		{in: "100", ok: true, want: 100 * time.Millisecond},
		{in: "", ok: false},
		{in: "1x", ok: false},
		{in: "-1", ok: false},
	}
	for _, tc := range tests {
		got, ok := parseRetry(tc.in)
		if ok != tc.ok {
			t.Fatalf("parseRetry(%q) ok=%v want %v", tc.in, ok, tc.ok)
		}
		if got != tc.want {
			t.Fatalf("parseRetry(%q)=%v want %v", tc.in, got, tc.want)
		}
	}
}
