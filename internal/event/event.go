// Package event defines the canonical change event emitted by every source
// and consumed by every sink in mycdc.
package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// Operation identifies the kind of row-level change that occurred.
type Operation string

const (
	OpInsert Operation = "insert"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

// Valid reports whether op is one of the known operations.
func (op Operation) Valid() bool {
	switch op {
	case OpInsert, OpUpdate, OpDelete:
		return true
	default:
		return false
	}
}

// Position identifies a source's replication cursor at the time an event was
// captured, so a pipeline can resume from exactly this point after a
// restart. Source implementations populate whichever fields are meaningful
// to them (MySQL sets File/Pos and, when GTID mode is enabled, GTID).
type Position struct {
	File string `json:"file,omitempty" yaml:"file,omitempty"`
	Pos  uint32 `json:"pos,omitempty" yaml:"pos,omitempty"`
	GTID string `json:"gtid,omitempty" yaml:"gtid,omitempty"`
}

// String renders the position for logs and the `mycdc status` command.
func (p Position) String() string {
	if p.GTID != "" {
		return p.GTID
	}
	if p.File == "" {
		return "<none>"
	}
	return fmt.Sprintf("%s:%d", p.File, p.Pos)
}

// IsZero reports whether the position carries no information at all, i.e.
// a source that has not yet captured anything.
func (p Position) IsZero() bool {
	return p.File == "" && p.GTID == ""
}

// ChangeEvent is the canonical representation of a single row-level change,
// independent of the upstream database technology. Sources are responsible
// for translating native change records (e.g. MySQL binlog row events) into
// ChangeEvent; sinks only ever see this shape.
type ChangeEvent struct {
	// Pipeline is the name of the pipeline (from config) that produced this
	// event, useful when multiple pipelines share a sink.
	Pipeline string `json:"pipeline"`

	// Database and Table identify the origin of the change.
	Database string `json:"database"`
	Table    string `json:"table"`

	Operation Operation `json:"operation"`

	// Before holds the row's column values prior to the change. It is nil
	// for inserts and, unless the source has full row images enabled, for
	// updates on engines that only log changed columns.
	Before map[string]any `json:"before,omitempty"`

	// After holds the row's column values after the change. It is nil for
	// deletes.
	After map[string]any `json:"after,omitempty"`

	// Position is the source's replication cursor immediately after this
	// event, i.e. "resume here to see the next event".
	Position Position `json:"position"`

	// Timestamp is when the source observed the change (for MySQL, the
	// binlog event's own timestamp).
	Timestamp time.Time `json:"timestamp"`
}

// Validate checks that the event is well-formed enough to hand to a sink.
func (e ChangeEvent) Validate() error {
	if e.Database == "" {
		return fmt.Errorf("event: database is required")
	}
	if e.Table == "" {
		return fmt.Errorf("event: table is required")
	}
	if !e.Operation.Valid() {
		return fmt.Errorf("event: invalid operation %q", e.Operation)
	}
	switch e.Operation {
	case OpInsert:
		if e.After == nil {
			return fmt.Errorf("event: insert requires After")
		}
	case OpDelete:
		if e.Before == nil {
			return fmt.Errorf("event: delete requires Before")
		}
	case OpUpdate:
		if e.Before == nil || e.After == nil {
			return fmt.Errorf("event: update requires Before and After")
		}
	}
	return nil
}

// QualifiedTable returns "database.table", the identifier used by filter
// patterns and log lines.
func (e ChangeEvent) QualifiedTable() string {
	return e.Database + "." + e.Table
}

// JSON renders the event as a single compact JSON line, the wire format
// used by the file and webhook sinks.
func (e ChangeEvent) JSON() ([]byte, error) {
	return json.Marshal(e)
}
