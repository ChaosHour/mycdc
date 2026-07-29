package state

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ChaosHour/mycdc/internal/event"
)

func TestFileStoreLoadMissingFileReturnsZeroValue(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "does-not-exist.json"))
	pos, err := store.Load("orders-cdc")
	if err != nil {
		t.Fatalf("Load() error = %v, want nil on first run", err)
	}
	if !pos.IsZero() {
		t.Errorf("Load() = %+v, want zero Position", pos)
	}
}

func TestFileStoreSaveAndLoadRoundTrip(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "state.json"))
	want := event.Position{File: "binlog.000123", Pos: 456}

	if err := store.Save("orders-cdc", want); err != nil {
		t.Fatalf("Save() error: %v", err)
	}
	got, err := store.Load("orders-cdc")
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if got != want {
		t.Errorf("Load() = %+v, want %+v", got, want)
	}
}

func TestFileStoreCreatesParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "dir", "state.json")
	store := NewFileStore(path)
	if err := store.Save("p", event.Position{File: "b", Pos: 1}); err != nil {
		t.Fatalf("Save() error: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected state file to exist: %v", err)
	}
}

func TestFileStoreMultiplePipelinesShareFile(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "state.json"))

	posA := event.Position{File: "binlog.a", Pos: 1}
	posB := event.Position{File: "binlog.b", Pos: 2}
	if err := store.Save("pipeline-a", posA); err != nil {
		t.Fatal(err)
	}
	if err := store.Save("pipeline-b", posB); err != nil {
		t.Fatal(err)
	}

	gotA, err := store.Load("pipeline-a")
	if err != nil {
		t.Fatal(err)
	}
	if gotA != posA {
		t.Errorf("pipeline-a = %+v, want %+v", gotA, posA)
	}
	gotB, err := store.Load("pipeline-b")
	if err != nil {
		t.Fatal(err)
	}
	if gotB != posB {
		t.Errorf("pipeline-b = %+v, want %+v", gotB, posB)
	}
}

func TestFileStoreOverwritesPreviousPosition(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "state.json"))
	if err := store.Save("p", event.Position{File: "binlog.000001", Pos: 100}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save("p", event.Position{File: "binlog.000002", Pos: 200}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load("p")
	if err != nil {
		t.Fatal(err)
	}
	want := event.Position{File: "binlog.000002", Pos: 200}
	if got != want {
		t.Errorf("Load() = %+v, want %+v", got, want)
	}
}

func TestFileStoreCorruptFileReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := NewFileStore(path)
	if _, err := store.Load("p"); err == nil {
		t.Fatal("expected error loading corrupt state file")
	}
}

func TestFileStoreEmptyFileTreatedAsNoState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	store := NewFileStore(path)
	pos, err := store.Load("p")
	if err != nil {
		t.Fatalf("Load() error = %v, want nil for empty file", err)
	}
	if !pos.IsZero() {
		t.Errorf("Load() = %+v, want zero Position", pos)
	}
}

func TestFileStoreDoesNotLeaveTempFilesBehind(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(filepath.Join(dir, "state.json"))
	if err := store.Save("p", event.Position{File: "b", Pos: 1}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.json" {
		t.Errorf("expected only state.json in %s, got %v", dir, entries)
	}
}

func TestFileStoreConcurrentSavesAreSerialized(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "state.json"))

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = store.Save("p", event.Position{File: "binlog", Pos: uint32(i)})
		}(i)
	}
	wg.Wait()

	// No assertion on which write "won" (last one, unordered) -- the point
	// is that concurrent Saves must not corrupt the file or race/panic.
	if _, err := store.Load("p"); err != nil {
		t.Fatalf("Load() error after concurrent saves: %v", err)
	}
}
