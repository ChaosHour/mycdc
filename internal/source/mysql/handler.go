package mysqlsource

import (
	"context"
	"sync"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/ChaosHour/mycdc/internal/event"
	"github.com/ChaosHour/mycdc/internal/source"
)

// canalEventHandler adapts canal's callback-based EventHandler interface to
// mycdc's source.EventHandler. It embeds canal.DummyEventHandler so it only
// needs to implement the callbacks it actually cares about.
//
// canal only reports a resumable position at transaction/statement
// boundaries (RotateEvent, XIDEvent, DDL), never mid-transaction. So every
// row event within a transaction is stamped with the position as of the
// *start* of that transaction: "if we crash now, resume here" -- which
// means the in-flight transaction's rows may be redelivered after a
// restart. That is the standard, expected at-least-once behavior for
// binlog-based CDC.
type canalEventHandler struct {
	canal.DummyEventHandler

	ctx     context.Context
	handler source.EventHandler

	mu  sync.Mutex
	pos event.Position
}

func newCanalEventHandler(ctx context.Context, handler source.EventHandler, start event.Position) *canalEventHandler {
	return &canalEventHandler{ctx: ctx, handler: handler, pos: start}
}

func (h *canalEventHandler) currentPos() event.Position {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pos
}

func (h *canalEventHandler) setPos(pos event.Position) {
	h.mu.Lock()
	h.pos = pos
	h.mu.Unlock()
}

func (h *canalEventHandler) OnRow(e *canal.RowsEvent) error {
	pos := h.currentPos()
	for _, ce := range convertRowsEvent(e, pos) {
		if err := h.handler(h.ctx, ce); err != nil {
			return err
		}
	}
	return nil
}

func (h *canalEventHandler) OnRotate(_ *replication.EventHeader, e *replication.RotateEvent) error {
	h.setPos(event.Position{File: string(e.NextLogName), Pos: uint32(e.Position)})
	return nil
}

func (h *canalEventHandler) OnPosSynced(_ *replication.EventHeader, pos gomysql.Position, set gomysql.GTIDSet, _ bool) error {
	gtid := ""
	if set != nil {
		gtid = set.String()
	}
	h.setPos(event.Position{File: pos.Name, Pos: pos.Pos, GTID: gtid})
	return nil
}

// OnTableNotFound overrides canal's default, which treats a row event for a
// table it can no longer introspect (e.g. dropped between the binlog event
// being written and canal fetching its schema) as a fatal error. mycdc
// treats it as "nothing to capture" and keeps streaming.
func (h *canalEventHandler) OnTableNotFound(_ *replication.EventHeader, _ *replication.RowsEvent) error {
	return nil
}

func (h *canalEventHandler) String() string { return "mycdc" }

var _ canal.EventHandler = (*canalEventHandler)(nil)
