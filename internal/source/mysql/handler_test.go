package mysqlsource

import (
	"context"
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/ChaosHour/mycdc/internal/event"
)

func TestCanalEventHandlerOnRowUsesCurrentPosition(t *testing.T) {
	var got []event.ChangeEvent
	h := newCanalEventHandler(context.Background(), func(_ context.Context, e event.ChangeEvent) error {
		got = append(got, e)
		return nil
	}, event.Position{File: "binlog.000001", Pos: 1})

	re := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.InsertAction,
		Rows:   [][]any{{int64(1), "new"}},
		Header: header(1700000000),
	}
	if err := h.OnRow(re); err != nil {
		t.Fatalf("OnRow() error: %v", err)
	}
	if len(got) != 1 || got[0].Position.File != "binlog.000001" {
		t.Errorf("expected event stamped with initial position, got %+v", got)
	}
}

func TestCanalEventHandlerOnRotateAdvancesPosition(t *testing.T) {
	h := newCanalEventHandler(context.Background(), func(_ context.Context, e event.ChangeEvent) error { return nil }, event.Position{})

	if err := h.OnRotate(nil, &replication.RotateEvent{NextLogName: []byte("binlog.000002"), Position: 4}); err != nil {
		t.Fatalf("OnRotate() error: %v", err)
	}
	if got := h.currentPos(); got.File != "binlog.000002" || got.Pos != 4 {
		t.Errorf("currentPos() = %+v, want binlog.000002:4", got)
	}
}

func TestCanalEventHandlerOnPosSyncedAdvancesPosition(t *testing.T) {
	h := newCanalEventHandler(context.Background(), func(_ context.Context, e event.ChangeEvent) error { return nil }, event.Position{})

	if err := h.OnPosSynced(nil, mysql.Position{Name: "binlog.000003", Pos: 999}, nil, false); err != nil {
		t.Fatalf("OnPosSynced() error: %v", err)
	}
	got := h.currentPos()
	if got.File != "binlog.000003" || got.Pos != 999 || got.GTID != "" {
		t.Errorf("currentPos() = %+v, want binlog.000003:999", got)
	}
}

func TestCanalEventHandlerSubsequentRowsUseUpdatedPosition(t *testing.T) {
	var positions []event.Position
	h := newCanalEventHandler(context.Background(), func(_ context.Context, e event.ChangeEvent) error {
		positions = append(positions, e.Position)
		return nil
	}, event.Position{File: "binlog.000001", Pos: 1})

	re := &canal.RowsEvent{Table: ordersTable(), Action: canal.InsertAction, Rows: [][]any{{int64(1), "new"}}, Header: header(1)}
	if err := h.OnRow(re); err != nil {
		t.Fatal(err)
	}

	// Simulate a transaction commit (XID) advancing the checkpointed
	// position; the next row event should be stamped with the new one.
	if err := h.OnPosSynced(nil, mysql.Position{Name: "binlog.000001", Pos: 500}, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := h.OnRow(re); err != nil {
		t.Fatal(err)
	}

	if len(positions) != 2 {
		t.Fatalf("got %d positions, want 2", len(positions))
	}
	if positions[0].Pos != 1 {
		t.Errorf("first row position = %+v, want Pos 1", positions[0])
	}
	if positions[1].Pos != 500 {
		t.Errorf("second row position = %+v, want Pos 500", positions[1])
	}
}

func TestCanalEventHandlerPropagatesHandlerError(t *testing.T) {
	wantErr := errors.New("sink unavailable")
	h := newCanalEventHandler(context.Background(), func(_ context.Context, e event.ChangeEvent) error {
		return wantErr
	}, event.Position{})

	re := &canal.RowsEvent{Table: ordersTable(), Action: canal.InsertAction, Rows: [][]any{{int64(1), "new"}}, Header: header(1)}
	err := h.OnRow(re)
	if !errors.Is(err, wantErr) {
		t.Errorf("OnRow() error = %v, want %v", err, wantErr)
	}
}

func TestCanalEventHandlerOnTableNotFoundIsNonFatal(t *testing.T) {
	h := newCanalEventHandler(context.Background(), func(_ context.Context, e event.ChangeEvent) error { return nil }, event.Position{})
	if err := h.OnTableNotFound(nil, nil); err != nil {
		t.Errorf("OnTableNotFound() error = %v, want nil", err)
	}
}

func TestCanalEventHandlerImplementsInterface(t *testing.T) {
	h := newCanalEventHandler(context.Background(), func(_ context.Context, e event.ChangeEvent) error { return nil }, event.Position{})
	if h.String() == "" {
		t.Error("String() should not be empty")
	}
	// Embedded DummyEventHandler no-ops should still be callable/satisfied.
	if err := h.OnDDL(nil, mysql.Position{}, nil); err != nil {
		t.Errorf("OnDDL() error: %v", err)
	}
}
