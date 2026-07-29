package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ChaosHour/mycdc/internal/event"
	"github.com/ChaosHour/mycdc/internal/filter"
	"github.com/ChaosHour/mycdc/internal/source"
)

// fakeSource streams a canned list of events to whatever handler Run is
// given, then either returns finishErr immediately or blocks until ctx is
// cancelled -- mirroring how a real, always-streaming binlog source
// behaves once it has caught up.
type fakeSource struct {
	events    []event.ChangeEvent
	delay     time.Duration
	finishErr error // if non-nil, returned right after the last event

	mu       sync.Mutex
	startPos event.Position
	closed   bool
}

func (f *fakeSource) Run(ctx context.Context, from event.Position, handler source.EventHandler) error {
	f.mu.Lock()
	f.startPos = from
	f.mu.Unlock()

	for _, e := range f.events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := handler(ctx, e); err != nil {
			return err
		}
		if f.delay > 0 {
			select {
			case <-time.After(f.delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if f.finishErr != nil {
		return f.finishErr
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSource) StartPos() event.Position {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.startPos
}

type fakeSink struct {
	mu     sync.Mutex
	writes []event.ChangeEvent
	err    error
	closed bool
}

func (f *fakeSink) Write(_ context.Context, e event.ChangeEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.writes = append(f.writes, e)
	return nil
}

func (f *fakeSink) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSink) Writes() []event.ChangeEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]event.ChangeEvent, len(f.writes))
	copy(out, f.writes)
	return out
}

type fakeState struct {
	mu        sync.Mutex
	preloaded map[string]event.Position
	saves     []event.Position
}

func newFakeState() *fakeState {
	return &fakeState{preloaded: make(map[string]event.Position)}
}

func (f *fakeState) Load(pipeline string) (event.Position, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.preloaded[pipeline], nil
}

func (f *fakeState) Save(pipeline string, pos event.Position) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves = append(f.saves, pos)
	return nil
}

func (f *fakeState) Saves() []event.Position {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]event.Position, len(f.saves))
	copy(out, f.saves)
	return out
}

func evt(table string, pos uint32) event.ChangeEvent {
	return event.ChangeEvent{
		Database:  "shop",
		Table:     table,
		Operation: event.OpInsert,
		After:     map[string]any{"id": 1},
		Position:  event.Position{File: "binlog.000001", Pos: pos},
		Timestamp: time.Now(),
	}
}

func TestNewValidatesOptions(t *testing.T) {
	valid := Options{
		Name:   "p",
		Source: &fakeSource{},
		Sink:   &fakeSink{},
		State:  newFakeState(),
	}

	cases := []struct {
		name   string
		mutate func(o Options) Options
	}{
		{"missing name", func(o Options) Options { o.Name = ""; return o }},
		{"missing source", func(o Options) Options { o.Source = nil; return o }},
		{"missing sink", func(o Options) Options { o.Sink = nil; return o }},
		{"missing state", func(o Options) Options { o.State = nil; return o }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(c.mutate(valid)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}

	if _, err := New(valid); err != nil {
		t.Fatalf("New() with valid options should succeed, got: %v", err)
	}
}

func TestPipelineFiltersEventsButStillCheckpoints(t *testing.T) {
	src := &fakeSource{
		events: []event.ChangeEvent{
			evt("orders", 100),
			evt("audit_log", 200),
		},
		finishErr: errors.New("stream ended"),
	}
	snk := &fakeSink{}
	st := newFakeState()
	f, err := filter.New([]string{"shop.orders"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	p, err := New(Options{
		Name:          "orders-cdc",
		Source:        src,
		Sink:          snk,
		State:         st,
		Filter:        f,
		FlushInterval: time.Hour, // rely on the final flush, not the ticker
	})
	if err != nil {
		t.Fatal(err)
	}

	err = p.Run(context.Background())
	if err == nil || err.Error() != "stream ended" {
		t.Fatalf("Run() error = %v, want \"stream ended\"", err)
	}

	writes := snk.Writes()
	if len(writes) != 1 || writes[0].Table != "orders" {
		t.Errorf("sink writes = %+v, want only the orders event", writes)
	}

	saves := st.Saves()
	if len(saves) == 0 {
		t.Fatal("expected a final checkpoint save")
	}
	last := saves[len(saves)-1]
	if last.Pos != 200 {
		t.Errorf("final checkpoint = %+v, want position 200 (checkpoint must advance past filtered events too)", last)
	}
}

func TestPipelineStampsPipelineName(t *testing.T) {
	src := &fakeSource{events: []event.ChangeEvent{evt("orders", 1)}, finishErr: errors.New("done")}
	snk := &fakeSink{}
	p, err := New(Options{Name: "orders-cdc", Source: src, Sink: snk, State: newFakeState()})
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Run(context.Background())

	writes := snk.Writes()
	if len(writes) != 1 || writes[0].Pipeline != "orders-cdc" {
		t.Errorf("writes = %+v, want Pipeline stamped to orders-cdc", writes)
	}
}

func TestPipelineLoadsCheckpointAndPassesToSource(t *testing.T) {
	src := &fakeSource{finishErr: errors.New("done")}
	st := newFakeState()
	st.preloaded["orders-cdc"] = event.Position{File: "binlog.000005", Pos: 999}

	p, err := New(Options{Name: "orders-cdc", Source: src, Sink: &fakeSink{}, State: st})
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Run(context.Background())

	if got := src.StartPos(); got != st.preloaded["orders-cdc"] {
		t.Errorf("source started from %+v, want %+v", got, st.preloaded["orders-cdc"])
	}
}

func TestPipelinePeriodicFlush(t *testing.T) {
	src := &fakeSource{
		events: []event.ChangeEvent{evt("orders", 1), evt("orders", 2), evt("orders", 3)},
		delay:  20 * time.Millisecond,
	}
	st := newFakeState()
	p, err := New(Options{
		Name:          "p",
		Source:        src,
		Sink:          &fakeSink{},
		State:         st,
		FlushInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)

	if len(st.Saves()) < 2 {
		t.Errorf("expected multiple periodic checkpoint saves, got %d", len(st.Saves()))
	}
}

func TestPipelineContextCancellationStopsRun(t *testing.T) {
	src := &fakeSource{} // no events, no finishErr: blocks on ctx.Done()
	p, err := New(Options{Name: "p", Source: src, Sink: &fakeSink{}, State: newFakeState()})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}
}

func TestPipelineSinkErrorStopsPipeline(t *testing.T) {
	src := &fakeSource{events: []event.ChangeEvent{evt("orders", 1)}}
	snk := &fakeSink{err: errors.New("webhook unreachable")}
	p, err := New(Options{Name: "p", Source: src, Sink: snk, State: newFakeState()})
	if err != nil {
		t.Fatal(err)
	}

	err = p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error propagated from sink")
	}
}

func TestPipelineClose(t *testing.T) {
	snk := &fakeSink{}
	p, err := New(Options{Name: "p", Source: &fakeSource{}, Sink: snk, State: newFakeState()})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if !snk.closed {
		t.Error("expected Close() to close the sink")
	}
}
