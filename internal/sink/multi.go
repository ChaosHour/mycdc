package sink

import (
	"context"
	"errors"

	"github.com/ChaosHour/mycdc/internal/event"
)

// MultiSink fans a single event out to several underlying sinks.
type MultiSink struct {
	sinks []Sink
}

// NewMulti combines sinks into one Sink that writes to all of them.
func NewMulti(sinks ...Sink) *MultiSink {
	return &MultiSink{sinks: sinks}
}

// Write delivers e to every underlying sink, continuing on to the rest even
// if one fails, and returns a combined error describing every failure.
func (m *MultiSink) Write(ctx context.Context, e event.ChangeEvent) error {
	var errs []error
	for _, s := range m.sinks {
		if err := s.Write(ctx, e); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close closes every underlying sink, continuing on to the rest even if one
// fails, and returns a combined error describing every failure.
func (m *MultiSink) Close() error {
	var errs []error
	for _, s := range m.sinks {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

var _ Sink = (*MultiSink)(nil)
