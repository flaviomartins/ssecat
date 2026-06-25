package output

import (
	"fmt"
	"io"

	"github.com/flaviomartins/ssecat/pkg/sse"
)

// Writer outputs parsed events.
type Writer interface {
	WriteEvent(event sse.Event) error
}

// PayloadWriter writes only event payloads.
type PayloadWriter struct {
	out io.Writer
}

// NewPayloadWriter returns a writer for default output mode.
func NewPayloadWriter(out io.Writer) *PayloadWriter {
	return &PayloadWriter{out: out}
}

// WriteEvent writes only event.Data to stdout.
func (w *PayloadWriter) WriteEvent(event sse.Event) error {
	_, err := fmt.Fprintln(w.out, event.Data)
	return err
}
