package sink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ChaosHour/mycdc/internal/event"
)

// FileSink appends events as JSON lines (one compact JSON object per line)
// to a local file, creating it and any missing parent directories if
// needed. Writes are OS-buffered for throughput; the file sink does not
// fsync on every event.
type FileSink struct {
	f  *os.File
	mu sync.Mutex
}

// NewFile opens (creating if necessary) path for appending.
func NewFile(path string) (*FileSink, error) {
	if path == "" {
		return nil, fmt.Errorf("file sink: path is required")
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("file sink: creating %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("file sink: opening %s: %w", path, err)
	}
	return &FileSink{f: f}, nil
}

func (s *FileSink) Write(_ context.Context, e event.ChangeEvent) error {
	b, err := e.JSON()
	if err != nil {
		return fmt.Errorf("file sink: encoding event: %w", err)
	}
	b = append(b, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.f.Write(b); err != nil {
		return fmt.Errorf("file sink: writing: %w", err)
	}
	return nil
}

func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

var _ Sink = (*FileSink)(nil)
