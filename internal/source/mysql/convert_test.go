package mysqlsource

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/ChaosHour/mycdc/internal/event"
)

func ordersTable() *schema.Table {
	return &schema.Table{
		Schema: "shop",
		Name:   "orders",
		Columns: []schema.TableColumn{
			{Name: "id"},
			{Name: "status"},
		},
	}
}

func header(ts uint32) *replication.EventHeader {
	return &replication.EventHeader{Timestamp: ts, ServerID: 42}
}

func TestConvertRowsEventInsert(t *testing.T) {
	pos := event.Position{File: "binlog.000001", Pos: 100}
	re := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.InsertAction,
		Rows: [][]any{
			{int64(1), "new"},
			{int64(2), "new"},
		},
		Header: header(1700000000),
	}

	events := convertRowsEvent(re, pos)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	for i, e := range events {
		if e.Database != "shop" || e.Table != "orders" {
			t.Errorf("event %d: wrong table identity: %+v", i, e)
		}
		if e.Operation != event.OpInsert {
			t.Errorf("event %d: Operation = %v, want insert", i, e.Operation)
		}
		if e.Before != nil {
			t.Errorf("event %d: insert should have nil Before, got %v", i, e.Before)
		}
		if e.Position != pos {
			t.Errorf("event %d: Position = %+v, want %+v", i, e.Position, pos)
		}
	}
	if events[0].After["id"] != int64(1) || events[0].After["status"] != "new" {
		t.Errorf("events[0].After = %+v", events[0].After)
	}
	if events[1].After["id"] != int64(2) {
		t.Errorf("events[1].After = %+v", events[1].After)
	}

	wantTS := time.Unix(1700000000, 0).UTC()
	if !events[0].Timestamp.Equal(wantTS) {
		t.Errorf("Timestamp = %v, want %v", events[0].Timestamp, wantTS)
	}
}

func TestConvertRowsEventDelete(t *testing.T) {
	re := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.DeleteAction,
		Rows: [][]any{
			{int64(1), "cancelled"},
		},
		Header: header(1700000000),
	}

	events := convertRowsEvent(re, event.Position{})
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	e := events[0]
	if e.Operation != event.OpDelete {
		t.Errorf("Operation = %v, want delete", e.Operation)
	}
	if e.After != nil {
		t.Errorf("delete should have nil After, got %v", e.After)
	}
	if e.Before["id"] != int64(1) || e.Before["status"] != "cancelled" {
		t.Errorf("Before = %+v", e.Before)
	}
}

func TestConvertRowsEventUpdate(t *testing.T) {
	re := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.UpdateAction,
		Rows: [][]any{
			{int64(1), "new"},     // before
			{int64(1), "shipped"}, // after
			{int64(2), "pending"}, // before
			{int64(2), "shipped"}, // after
		},
		Header: header(1700000000),
	}

	events := convertRowsEvent(re, event.Position{})
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (two before/after pairs)", len(events))
	}
	if events[0].Operation != event.OpUpdate {
		t.Errorf("Operation = %v, want update", events[0].Operation)
	}
	if events[0].Before["status"] != "new" || events[0].After["status"] != "shipped" {
		t.Errorf("events[0] before/after = %+v / %+v", events[0].Before, events[0].After)
	}
	if events[1].Before["status"] != "pending" || events[1].After["status"] != "shipped" {
		t.Errorf("events[1] before/after = %+v / %+v", events[1].Before, events[1].After)
	}
}

func TestConvertRowsEventUpdateOddRowsIgnoresTrailingRow(t *testing.T) {
	// Malformed/truncated update event: an odd number of rows means the
	// last one has no pair. We should not panic, just drop the incomplete
	// trailing row.
	re := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.UpdateAction,
		Rows: [][]any{
			{int64(1), "new"},
			{int64(1), "shipped"},
			{int64(2), "pending"}, // no matching "after" row
		},
		Header: header(1700000000),
	}

	events := convertRowsEvent(re, event.Position{})
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
}

func TestConvertRowsEventUnknownActionReturnsNil(t *testing.T) {
	re := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: "truncate",
		Rows:   [][]any{{int64(1)}},
		Header: header(1700000000),
	}
	if got := convertRowsEvent(re, event.Position{}); got != nil {
		t.Errorf("got %v, want nil for unknown action", got)
	}
}

func TestConvertRowsEventMissingColumnsHandledGracefully(t *testing.T) {
	// A row with fewer values than the table has columns (schema drift
	// between the binlog event and the metadata canal fetched) should not
	// panic; the extra columns are simply absent from the map.
	re := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.InsertAction,
		Rows:   [][]any{{int64(1)}}, // missing "status"
		Header: header(1700000000),
	}
	events := convertRowsEvent(re, event.Position{})
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if _, ok := events[0].After["status"]; ok {
		t.Errorf("expected status to be absent, got %+v", events[0].After)
	}
	if events[0].After["id"] != int64(1) {
		t.Errorf("After = %+v", events[0].After)
	}
}

func TestEventTimestampFallsBackToNowWhenZero(t *testing.T) {
	before := time.Now().UTC()
	ts := eventTimestamp(header(0))
	after := time.Now().UTC()

	if ts.Before(before) || ts.After(after) {
		t.Errorf("eventTimestamp fallback = %v, want between %v and %v", ts, before, after)
	}
}

func TestEventTimestampNilHeader(t *testing.T) {
	before := time.Now().UTC()
	ts := eventTimestamp(nil)
	if ts.Before(before) {
		t.Errorf("eventTimestamp(nil) = %v, want a recent timestamp", ts)
	}
}
