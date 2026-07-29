// Package mysqlsource implements source.Source for MySQL 5.7 through 9.x
// using row-based binlog replication (the same protocol real replicas use),
// via the github.com/go-mysql-org/go-mysql canal client.
package mysqlsource

import (
	"context"
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/ChaosHour/mycdc/internal/config"
	"github.com/ChaosHour/mycdc/internal/event"
	"github.com/ChaosHour/mycdc/internal/source"
)

// Source captures row-level changes from a MySQL server's binlog.
type Source struct {
	cfg   config.MySQLSource
	canal *canal.Canal
}

// New connects to the MySQL server described by cfg and prepares a
// replication client. The server must have binlog_format=ROW and the
// configured user must have REPLICATION SLAVE and REPLICATION CLIENT
// privileges. It does not start streaming until Run is called.
func New(cfg config.MySQLSource) (*Source, error) {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	canalCfg.User = cfg.User
	canalCfg.Password = cfg.Password
	canalCfg.ServerID = cfg.ServerID
	canalCfg.Flavor = cfg.Flavor
	canalCfg.Charset = cfg.Charset
	canalCfg.HeartbeatPeriod = cfg.HeartbeatPeriod.Duration
	canalCfg.ReadTimeout = cfg.ReadTimeout.Duration
	// mycdc streams the binlog directly and never needs mysqldump's
	// consistent-snapshot dump: an operator either points it at a fresh
	// server (starts from the current binlog tip) or resumes from a saved
	// checkpoint.
	canalCfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(canalCfg)
	if err != nil {
		return nil, fmt.Errorf("mysql source: connecting to %s: %w", canalCfg.Addr, err)
	}
	return &Source{cfg: cfg, canal: c}, nil
}

// Run streams binlog events starting from from, or from the server's
// current position if from is the zero Position (first run, no checkpoint
// yet). It blocks until ctx is cancelled or an unrecoverable error occurs.
func (s *Source) Run(ctx context.Context, from event.Position, handler source.EventHandler) error {
	s.canal.SetEventHandler(newCanalEventHandler(ctx, handler, from))

	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			s.canal.Close()
		case <-stopped:
		}
	}()

	runErr := s.startFrom(from)

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return runErr
}

func (s *Source) startFrom(from event.Position) error {
	switch {
	case from.GTID != "":
		set, err := gomysql.ParseGTIDSet(s.cfg.Flavor, from.GTID)
		if err != nil {
			return fmt.Errorf("mysql source: parsing GTID set %q: %w", from.GTID, err)
		}
		return s.canal.StartFromGTID(set)

	case from.File != "":
		return s.canal.RunFrom(gomysql.Position{Name: from.File, Pos: from.Pos})

	default:
		pos, err := s.canal.GetMasterPos()
		if err != nil {
			return fmt.Errorf("mysql source: fetching current master position: %w", err)
		}
		return s.canal.RunFrom(pos)
	}
}

// Close releases the underlying replication connection.
func (s *Source) Close() error {
	s.canal.Close()
	return nil
}

var _ source.Source = (*Source)(nil)
