package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/ChaosHour/mycdc/internal/event"
)

func TestFileSinkWritesJSONLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out", "events.jsonl")
	s, err := NewFile(path)
	if err != nil {
		t.Fatalf("NewFile() error: %v", err)
	}
	defer s.Close()

	for i := range 3 {
		e := sampleInsert()
		e.After["id"] = float64(i)
		if err := s.Write(context.Background(), e); err != nil {
			t.Fatalf("Write() error: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening output file: %v", err)
	}
	defer f.Close()

	var count int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var decoded event.ChangeEvent
		if err := json.Unmarshal(scanner.Bytes(), &decoded); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", count, err)
		}
		if decoded.After["id"].(float64) != float64(count) {
			t.Errorf("line %d: After[id] = %v, want %d", count, decoded.After["id"], count)
		}
		count++
	}
	if count != 3 {
		t.Errorf("wrote %d lines, want 3", count)
	}
}

func TestFileSinkAppendsAcrossReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	s1, err := NewFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Write(context.Background(), sampleInsert()); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.Write(context.Background(), sampleInsert()); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	if lines != 2 {
		t.Errorf("got %d lines after reopen, want 2", lines)
	}
}

func TestFileSinkRequiresPath(t *testing.T) {
	if _, err := NewFile(""); err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestFileSinkCreatesParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "c", "events.jsonl")
	s, err := NewFile(path)
	if err != nil {
		t.Fatalf("NewFile() error: %v", err)
	}
	defer s.Close()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("expected file to exist: %v", err)
	}
}
