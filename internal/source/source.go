// Package source defines the interface every change-capture source
// implements, independent of the upstream database technology.
package source

import (
	"context"

	"github.com/ChaosHour/mycdc/internal/event"
)

// EventHandler processes a single captured change. Returning a non-nil
// error tells the Source that the event was not durably handled; sources
// should stop and propagate the error rather than advancing past it.
type EventHandler func(ctx context.Context, e event.ChangeEvent) error

// Source streams change events from an upstream database.
type Source interface {
	// Run streams events starting immediately after from (the zero
	// Position means "start from the source's current position", i.e. no
	// prior checkpoint exists) and invokes handler for each one, in order,
	// until ctx is cancelled or an unrecoverable error occurs. Run returns
	// ctx.Err() on ordinary cancellation and a non-nil error otherwise.
	Run(ctx context.Context, from event.Position, handler EventHandler) error

	// Close releases the source's underlying connection.
	Close() error
}
