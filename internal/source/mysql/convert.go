package mysqlsource

import (
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/ChaosHour/mycdc/internal/event"
)

// convertRowsEvent translates one canal RowsEvent into the ChangeEvents it
// represents. A single binlog row event can bundle several logical row
// changes (e.g. a multi-row INSERT, or an UPDATE whose Rows alternate
// [before, after] pairs), so this returns a slice.
//
// This is a pure function deliberately kept free of any canal/network
// dependency beyond the plain structs canal already hands us, so it can be
// unit tested without a live MySQL server.
func convertRowsEvent(e *canal.RowsEvent, pos event.Position) []event.ChangeEvent {
	ts := eventTimestamp(e.Header)
	switch e.Action {
	case canal.InsertAction:
		out := make([]event.ChangeEvent, 0, len(e.Rows))
		for _, row := range e.Rows {
			out = append(out, event.ChangeEvent{
				Database:  e.Table.Schema,
				Table:     e.Table.Name,
				Operation: event.OpInsert,
				After:     rowToMap(e.Table, row),
				Position:  pos,
				Timestamp: ts,
			})
		}
		return out

	case canal.DeleteAction:
		out := make([]event.ChangeEvent, 0, len(e.Rows))
		for _, row := range e.Rows {
			out = append(out, event.ChangeEvent{
				Database:  e.Table.Schema,
				Table:     e.Table.Name,
				Operation: event.OpDelete,
				Before:    rowToMap(e.Table, row),
				Position:  pos,
				Timestamp: ts,
			})
		}
		return out

	case canal.UpdateAction:
		out := make([]event.ChangeEvent, 0, len(e.Rows)/2)
		for i := 0; i+1 < len(e.Rows); i += 2 {
			out = append(out, event.ChangeEvent{
				Database:  e.Table.Schema,
				Table:     e.Table.Name,
				Operation: event.OpUpdate,
				Before:    rowToMap(e.Table, e.Rows[i]),
				After:     rowToMap(e.Table, e.Rows[i+1]),
				Position:  pos,
				Timestamp: ts,
			})
		}
		return out

	default:
		return nil
	}
}

// rowToMap zips a raw binlog row (positional values) with the table's
// column names.
func rowToMap(table *schema.Table, row []any) map[string]any {
	m := make(map[string]any, len(table.Columns))
	for i, col := range table.Columns {
		if i >= len(row) {
			break
		}
		m[col.Name] = row[i]
	}
	return m
}

func eventTimestamp(h *replication.EventHeader) time.Time {
	if h == nil || h.Timestamp == 0 {
		return time.Now().UTC()
	}
	return time.Unix(int64(h.Timestamp), 0).UTC()
}
