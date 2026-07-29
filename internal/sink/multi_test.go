package sink

import (
	"context"
	"errors"
	"testing"

	"github.com/ChaosHour/mycdc/internal/event"
)

type fakeSink struct {
	writes   []event.ChangeEvent
	writeErr error
	closed   bool
	closeErr error
}

func (f *fakeSink) Write(_ context.Context, e event.ChangeEvent) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	f.writes = append(f.writes, e)
	return nil
}

func (f *fakeSink) Close() error {
	f.closed = true
	return f.closeErr
}

func TestMultiSinkWritesToAll(t *testing.T) {
	a, b := &fakeSink{}, &fakeSink{}
	m := NewMulti(a, b)

	if err := m.Write(context.Background(), sampleInsert()); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	if len(a.writes) != 1 || len(b.writes) != 1 {
		t.Errorf("expected both sinks to receive the event: a=%d b=%d", len(a.writes), len(b.writes))
	}
}

func TestMultiSinkContinuesAfterOneSinkFails(t *testing.T) {
	failing := &fakeSink{writeErr: errors.New("boom")}
	ok := &fakeSink{}
	m := NewMulti(failing, ok)

	err := m.Write(context.Background(), sampleInsert())
	if err == nil {
		t.Fatal("expected combined error")
	}
	if len(ok.writes) != 1 {
		t.Error("expected the healthy sink to still receive the event")
	}
}

func TestMultiSinkCloseClosesAll(t *testing.T) {
	a, b := &fakeSink{}, &fakeSink{closeErr: errors.New("close failed")}
	m := NewMulti(a, b)

	err := m.Close()
	if err == nil {
		t.Fatal("expected combined close error")
	}
	if !a.closed || !b.closed {
		t.Errorf("expected both sinks closed: a=%v b=%v", a.closed, b.closed)
	}
}

func TestMultiSinkEmpty(t *testing.T) {
	m := NewMulti()
	if err := m.Write(context.Background(), sampleInsert()); err != nil {
		t.Errorf("Write() on empty MultiSink should succeed, got: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Errorf("Close() on empty MultiSink should succeed, got: %v", err)
	}
}
