// Package state persists replication checkpoints so a pipeline can resume
// from where it left off after a restart instead of re-scanning the whole
// binlog.
package state

import "github.com/ChaosHour/mycdc/internal/event"

// Store loads and saves a pipeline's last-captured replication position.
type Store interface {
	// Load returns the last saved position for pipeline. If nothing has
	// been saved yet (first run), it returns the zero Position and a nil
	// error — that is not an error condition.
	Load(pipeline string) (event.Position, error)

	// Save persists pos as pipeline's new checkpoint. It must be safe to
	// call repeatedly and must not leave a partially written file behind
	// if interrupted.
	Save(pipeline string, pos event.Position) error
}
