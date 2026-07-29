// Package sink defines destinations that captured change events are
// written to, and the built-in stdout, file, and webhook implementations.
package sink

import (
	"context"
	"fmt"
	"os"

	"github.com/ChaosHour/mycdc/internal/config"
	"github.com/ChaosHour/mycdc/internal/event"
)

// Sink is a destination for captured change events.
type Sink interface {
	// Write delivers a single event. Implementations should return an
	// error only for failures the caller should treat as this event not
	// having been delivered (the pipeline will not advance its checkpoint
	// past an event whose Write failed).
	Write(ctx context.Context, e event.ChangeEvent) error

	// Close releases any resources (open files, HTTP connections) held by
	// the sink. It is called once when the pipeline shuts down.
	Close() error
}

// New builds the Sink described by cfg.
func New(cfg config.SinkConfig) (Sink, error) {
	switch cfg.Type {
	case "stdout":
		return NewStdout(os.Stdout, cfg.Format), nil
	case "file":
		return NewFile(cfg.Path)
	case "webhook":
		return NewWebhook(WebhookConfig{
			URL:          cfg.URL,
			Headers:      cfg.Headers,
			Timeout:      cfg.Timeout.Duration,
			MaxRetries:   cfg.MaxRetries,
			RetryBackoff: cfg.RetryBackoff.Duration,
		}), nil
	default:
		return nil, fmt.Errorf("sink: unknown type %q", cfg.Type)
	}
}

// NewAll builds one Sink per entry in cfgs and combines them into a single
// fan-out Sink.
func NewAll(cfgs []config.SinkConfig) (Sink, error) {
	sinks := make([]Sink, 0, len(cfgs))
	for i, cfg := range cfgs {
		s, err := New(cfg)
		if err != nil {
			return nil, fmt.Errorf("sink[%d]: %w", i, err)
		}
		sinks = append(sinks, s)
	}
	return NewMulti(sinks...), nil
}
