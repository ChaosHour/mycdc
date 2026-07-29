package sink

import (
	"path/filepath"
	"testing"

	"github.com/ChaosHour/mycdc/internal/config"
)

func TestNewStdoutFromConfig(t *testing.T) {
	s, err := New(config.SinkConfig{Type: "stdout", Format: "json"})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if _, ok := s.(*StdoutSink); !ok {
		t.Errorf("New() = %T, want *StdoutSink", s)
	}
}

func TestNewFileFromConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s, err := New(config.SinkConfig{Type: "file", Path: path})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer s.Close()
	if _, ok := s.(*FileSink); !ok {
		t.Errorf("New() = %T, want *FileSink", s)
	}
}

func TestNewWebhookFromConfig(t *testing.T) {
	s, err := New(config.SinkConfig{Type: "webhook", URL: "https://example.com/cdc"})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if _, ok := s.(*WebhookSink); !ok {
		t.Errorf("New() = %T, want *WebhookSink", s)
	}
}

func TestNewUnknownType(t *testing.T) {
	if _, err := New(config.SinkConfig{Type: "carrier-pigeon"}); err == nil {
		t.Fatal("expected error for unknown sink type")
	}
}

func TestNewAllBuildsMultiSink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s, err := NewAll([]config.SinkConfig{
		{Type: "stdout", Format: "json"},
		{Type: "file", Path: path},
	})
	if err != nil {
		t.Fatalf("NewAll() error: %v", err)
	}
	defer s.Close()
	if _, ok := s.(*MultiSink); !ok {
		t.Errorf("NewAll() = %T, want *MultiSink", s)
	}
}

func TestNewAllPropagatesError(t *testing.T) {
	if _, err := NewAll([]config.SinkConfig{{Type: "file", Path: ""}}); err == nil {
		t.Fatal("expected error to propagate from NewAll")
	}
}
