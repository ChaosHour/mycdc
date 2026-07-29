package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ChaosHour/mycdc/internal/event"
)

// FileStore is a Store backed by a single local JSON file. Multiple
// pipelines may share one file; each is tracked under its own key. Writes
// are atomic (write-to-temp-then-rename) so a crash mid-write can never
// leave a corrupt or half-written state file.
type FileStore struct {
	path string
	mu   sync.Mutex
}

// NewFileStore returns a FileStore that reads and writes path. The parent
// directory is created on first Save if it does not already exist.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

type fileFormat struct {
	Pipelines map[string]event.Position `json:"pipelines"`
	UpdatedAt time.Time                 `json:"updated_at"`
}

func (s *FileStore) Load(pipeline string) (event.Position, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.read()
	if err != nil {
		return event.Position{}, err
	}
	return f.Pipelines[pipeline], nil
}

func (s *FileStore) Save(pipeline string, pos event.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.read()
	if err != nil {
		return err
	}
	if f.Pipelines == nil {
		f.Pipelines = make(map[string]event.Position)
	}
	f.Pipelines[pipeline] = pos
	f.UpdatedAt = time.Now().UTC()
	return s.write(f)
}

func (s *FileStore) read() (fileFormat, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return fileFormat{}, nil
	}
	if err != nil {
		return fileFormat{}, fmt.Errorf("state: reading %s: %w", s.path, err)
	}
	if len(data) == 0 {
		return fileFormat{}, nil
	}
	var f fileFormat
	if err := json.Unmarshal(data, &f); err != nil {
		return fileFormat{}, fmt.Errorf("state: %s is corrupt: %w", s.path, err)
	}
	return f, nil
}

// write atomically replaces the state file: it writes the new contents to a
// temp file in the same directory, fsyncs it, then renames it into place.
// Rename is atomic on both POSIX and Windows, so a reader (or a crash) never
// observes a partially written file.
func (s *FileStore) write(f fileFormat) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("state: creating %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("state: encoding: %w", err)
	}

	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("state: creating temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // no-op once the rename below succeeds

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("state: writing temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("state: syncing temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("state: closing temp file: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("state: renaming into place: %w", err)
	}
	return nil
}

var _ Store = (*FileStore)(nil)
