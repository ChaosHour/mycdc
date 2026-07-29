package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ChaosHour/mycdc/internal/event"
)

func sampleInsert() event.ChangeEvent {
	return event.ChangeEvent{
		Pipeline:  "orders-cdc",
		Database:  "shop",
		Table:     "orders",
		Operation: event.OpInsert,
		After:     map[string]any{"id": float64(1), "status": "new"},
		Position:  event.Position{File: "binlog.000001", Pos: 100},
		Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
}

func TestStdoutSinkJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdout(&buf, "json")

	if err := s.Write(context.Background(), sampleInsert()); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	line := strings.TrimSpace(buf.String())
	var decoded event.ChangeEvent
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, buf.String())
	}
	if decoded.Table != "orders" {
		t.Errorf("decoded.Table = %q, want orders", decoded.Table)
	}
}

func TestStdoutSinkDefaultsToJSON(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdout(&buf, "")
	if s.format != "json" {
		t.Errorf("format = %q, want json", s.format)
	}
}

func TestStdoutSinkPrettyFormat(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdout(&buf, "pretty")

	if err := s.Write(context.Background(), sampleInsert()); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "insert") {
		t.Errorf("pretty output missing operation: %s", out)
	}
	if !strings.Contains(out, "shop.orders") {
		t.Errorf("pretty output missing qualified table: %s", out)
	}
	if !strings.Contains(out, "binlog.000001:100") {
		t.Errorf("pretty output missing position: %s", out)
	}
	if !strings.Contains(out, "after") {
		t.Errorf("pretty output missing after row: %s", out)
	}
	if strings.Contains(out, "before") {
		t.Errorf("pretty output should not print before for an insert: %s", out)
	}
}

func TestStdoutSinkPrettyFormatUpdateShowsBeforeAndAfter(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdout(&buf, "pretty")

	e := sampleInsert()
	e.Operation = event.OpUpdate
	e.Before = map[string]any{"id": float64(1), "status": "new"}
	e.After = map[string]any{"id": float64(1), "status": "shipped"}

	if err := s.Write(context.Background(), e); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "before") || !strings.Contains(out, "after") {
		t.Errorf("pretty output should show both before and after for an update: %s", out)
	}
}

func TestStdoutSinkClose(t *testing.T) {
	s := NewStdout(&bytes.Buffer{}, "json")
	if err := s.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}

func TestStdoutSinkMultipleWritesEachOnOwnLine(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdout(&buf, "json")

	for range 3 {
		if err := s.Write(context.Background(), sampleInsert()); err != nil {
			t.Fatal(err)
		}
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Errorf("got %d lines, want 3", len(lines))
	}
}
