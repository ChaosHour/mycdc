// Package pipeline wires a Source, a table filter, a Sink, and a state
// Store together into a running capture pipeline.
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ChaosHour/mycdc/internal/event"
	"github.com/ChaosHour/mycdc/internal/filter"
	"github.com/ChaosHour/mycdc/internal/sink"
	"github.com/ChaosHour/mycdc/internal/source"
	"github.com/ChaosHour/mycdc/internal/state"
)

const defaultFlushInterval = 5 * time.Second

// Options configures a Pipeline.
type Options struct {
	// Name identifies the pipeline; it is stamped onto every event and
	// used as the state store's checkpoint key.
	Name string

	Source source.Source
	Sink   sink.Sink
	State  state.Store

	// Filter selects which tables produce events. A nil Filter allows
	// every table.
	Filter *filter.TableFilter

	// FlushInterval controls how often the in-memory checkpoint is
	// persisted to State. Smaller values bound how much gets redelivered
	// after a crash, at the cost of more frequent (and, for the file
	// store, fsync'd) writes. Defaults to 5s.
	FlushInterval time.Duration

	Logger *slog.Logger
}

// Pipeline reads change events from a Source, drops any the table filter
// rejects, writes the rest to a Sink, and periodically checkpoints
// replication progress to a state Store so a restart resumes rather than
// re-scanning.
type Pipeline struct {
	opts Options
}

// New validates opts and returns a ready-to-run Pipeline.
func New(opts Options) (*Pipeline, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("pipeline: name is required")
	}
	if opts.Source == nil {
		return nil, fmt.Errorf("pipeline: source is required")
	}
	if opts.Sink == nil {
		return nil, fmt.Errorf("pipeline: sink is required")
	}
	if opts.State == nil {
		return nil, fmt.Errorf("pipeline: state store is required")
	}
	if opts.Filter == nil {
		f, err := filter.New(nil, nil)
		if err != nil {
			return nil, err
		}
		opts.Filter = f
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Pipeline{opts: opts}, nil
}

// Run loads the last checkpoint, streams events from the source until ctx
// is cancelled or the source returns an error, and checkpoints progress
// along the way. It always attempts one final checkpoint save before
// returning, even on error, so a clean shutdown loses no progress.
func (p *Pipeline) Run(ctx context.Context) error {
	log := p.opts.Logger.With("pipeline", p.opts.Name)

	startPos, err := p.opts.State.Load(p.opts.Name)
	if err != nil {
		return fmt.Errorf("pipeline: loading checkpoint: %w", err)
	}
	log.Info("starting pipeline", "from_position", startPos.String())

	var (
		mu      sync.Mutex
		pending event.Position
		dirty   bool
	)

	flush := func() {
		mu.Lock()
		if !dirty {
			mu.Unlock()
			return
		}
		pos := pending
		dirty = false
		mu.Unlock()

		if err := p.opts.State.Save(p.opts.Name, pos); err != nil {
			log.Error("failed to save checkpoint", "error", err, "position", pos.String())
			return
		}
		log.Debug("saved checkpoint", "position", pos.String())
	}

	handler := func(hctx context.Context, e event.ChangeEvent) error {
		e.Pipeline = p.opts.Name

		if p.opts.Filter.Allows(e.Database, e.Table) {
			if err := e.Validate(); err != nil {
				return fmt.Errorf("pipeline: invalid event from source: %w", err)
			}
			if err := p.opts.Sink.Write(hctx, e); err != nil {
				return fmt.Errorf("pipeline: writing to sink: %w", err)
			}
		}

		// The checkpoint advances even for filtered-out events: it tracks
		// "how far into the binlog have we processed", not "how many
		// events did we deliver". Otherwise excluding a table would pin
		// the checkpoint in place and force an ever-growing re-scan on
		// every restart.
		mu.Lock()
		pending = e.Position
		dirty = true
		mu.Unlock()
		return nil
	}

	ticker := time.NewTicker(p.opts.FlushInterval)
	defer ticker.Stop()

	errCh := make(chan error, 1)
	go func() { errCh <- p.opts.Source.Run(ctx, startPos, handler) }()

	for {
		select {
		case <-ticker.C:
			flush()
		case runErr := <-errCh:
			flush()
			log.Info("pipeline stopped", "error", runErr)
			return runErr
		}
	}
}

// Close releases the pipeline's sink. The source and state store are owned
// by the caller (typically the CLI's `run` command) and are closed
// separately, since they may be constructed before the Pipeline and need
// to outlive a failed Pipeline construction.
func (p *Pipeline) Close() error {
	return p.opts.Sink.Close()
}
