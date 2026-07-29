package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestOperationValid(t *testing.T) {
	cases := []struct {
		op   Operation
		want bool
	}{
		{OpInsert, true},
		{OpUpdate, true},
		{OpDelete, true},
		{Operation("truncate"), false},
		{Operation(""), false},
	}
	for _, c := range cases {
		if got := c.op.Valid(); got != c.want {
			t.Errorf("Operation(%q).Valid() = %v, want %v", c.op, got, c.want)
		}
	}
}

func TestPositionString(t *testing.T) {
	cases := []struct {
		name string
		pos  Position
		want string
	}{
		{"zero", Position{}, "<none>"},
		{"file and pos", Position{File: "binlog.000123", Pos: 456}, "binlog.000123:456"},
		{"gtid preferred", Position{File: "binlog.000123", Pos: 456, GTID: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"}, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.pos.String(); got != c.want {
				t.Errorf("String() = %q, want %q", got, c.want)
			}
		})
	}
}

func TestPositionIsZero(t *testing.T) {
	if !(Position{}).IsZero() {
		t.Error("empty Position should be zero")
	}
	if (Position{File: "binlog.000001"}).IsZero() {
		t.Error("Position with File should not be zero")
	}
	if (Position{GTID: "abc:1"}).IsZero() {
		t.Error("Position with GTID should not be zero")
	}
}

func validInsert() ChangeEvent {
	return ChangeEvent{
		Pipeline:  "orders",
		Database:  "shop",
		Table:     "orders",
		Operation: OpInsert,
		After:     map[string]any{"id": 1},
		Position:  Position{File: "binlog.000001", Pos: 100},
		Timestamp: time.Now(),
	}
}

func TestChangeEventValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(e ChangeEvent) ChangeEvent
		wantErr bool
	}{
		{"valid insert", func(e ChangeEvent) ChangeEvent { return e }, false},
		{"missing database", func(e ChangeEvent) ChangeEvent { e.Database = ""; return e }, true},
		{"missing table", func(e ChangeEvent) ChangeEvent { e.Table = ""; return e }, true},
		{"invalid operation", func(e ChangeEvent) ChangeEvent { e.Operation = "truncate"; return e }, true},
		{"insert missing after", func(e ChangeEvent) ChangeEvent {
			e.Operation = OpInsert
			e.After = nil
			return e
		}, true},
		{"delete missing before", func(e ChangeEvent) ChangeEvent {
			e.Operation = OpDelete
			e.Before = nil
			e.After = nil
			return e
		}, true},
		{"delete with before ok", func(e ChangeEvent) ChangeEvent {
			e.Operation = OpDelete
			e.Before = map[string]any{"id": 1}
			e.After = nil
			return e
		}, false},
		{"update missing before", func(e ChangeEvent) ChangeEvent {
			e.Operation = OpUpdate
			e.Before = nil
			e.After = map[string]any{"id": 1}
			return e
		}, true},
		{"update missing after", func(e ChangeEvent) ChangeEvent {
			e.Operation = OpUpdate
			e.Before = map[string]any{"id": 1}
			e.After = nil
			return e
		}, true},
		{"update ok", func(e ChangeEvent) ChangeEvent {
			e.Operation = OpUpdate
			e.Before = map[string]any{"id": 1, "status": "new"}
			e.After = map[string]any{"id": 1, "status": "shipped"}
			return e
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := c.mutate(validInsert())
			err := e.Validate()
			if (err != nil) != c.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, c.wantErr)
			}
		})
	}
}

func TestQualifiedTable(t *testing.T) {
	e := ChangeEvent{Database: "shop", Table: "orders"}
	if got, want := e.QualifiedTable(), "shop.orders"; got != want {
		t.Errorf("QualifiedTable() = %q, want %q", got, want)
	}
}

func TestChangeEventJSONRoundTrip(t *testing.T) {
	e := validInsert()
	b, err := e.JSON()
	if err != nil {
		t.Fatalf("JSON() error: %v", err)
	}

	var decoded ChangeEvent
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	if decoded.Database != e.Database || decoded.Table != e.Table || decoded.Operation != e.Operation {
		t.Errorf("round trip mismatch: got %+v, want %+v", decoded, e)
	}
	if decoded.After["id"].(float64) != 1 {
		t.Errorf("round trip After mismatch: got %+v", decoded.After)
	}
}

func TestChangeEventJSONOmitsEmptyBeforeAfter(t *testing.T) {
	e := validInsert()
	b, err := e.JSON()
	if err != nil {
		t.Fatalf("JSON() error: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	if _, ok := raw["before"]; ok {
		t.Errorf("expected \"before\" to be omitted for insert, got raw JSON: %s", b)
	}
}
