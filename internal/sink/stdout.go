package sink

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/ChaosHour/mycdc/internal/event"
)

// StdoutSink writes events to a writer (normally os.Stdout) as either
// compact JSON lines or a human-readable "pretty" summary.
type StdoutSink struct {
	w      io.Writer
	format string
	mu     sync.Mutex
}

// NewStdout returns a StdoutSink writing to w. format is "json" (default)
// or "pretty".
func NewStdout(w io.Writer, format string) *StdoutSink {
	if format == "" {
		format = "json"
	}
	return &StdoutSink{w: w, format: format}
}

func (s *StdoutSink) Write(_ context.Context, e event.ChangeEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.format == "pretty" {
		return s.writePretty(e)
	}
	return s.writeJSON(e)
}

func (s *StdoutSink) writeJSON(e event.ChangeEvent) error {
	b, err := e.JSON()
	if err != nil {
		return fmt.Errorf("stdout sink: encoding event: %w", err)
	}
	_, err = fmt.Fprintf(s.w, "%s\n", b)
	return err
}

func (s *StdoutSink) writePretty(e event.ChangeEvent) error {
	if _, err := fmt.Fprintf(s.w, "%-6s %s  (%s)\n",
		string(e.Operation), e.QualifiedTable(), e.Position); err != nil {
		return err
	}
	if e.Before != nil {
		if err := s.writeRow("  before", e.Before); err != nil {
			return err
		}
	}
	if e.After != nil {
		if err := s.writeRow("  after ", e.After); err != nil {
			return err
		}
	}
	return nil
}

func (s *StdoutSink) writeRow(label string, row map[string]any) error {
	b, err := marshalRow(row)
	if err != nil {
		return fmt.Errorf("stdout sink: encoding row: %w", err)
	}
	_, err = fmt.Fprintf(s.w, "%s: %s\n", label, b)
	return err
}

func (s *StdoutSink) Close() error { return nil }

var _ Sink = (*StdoutSink)(nil)
