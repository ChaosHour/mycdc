package cmd

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/ChaosHour/mycdc/internal/config"
	"github.com/ChaosHour/mycdc/internal/filter"
	"github.com/ChaosHour/mycdc/internal/pipeline"
	"github.com/ChaosHour/mycdc/internal/sink"
	mysqlsource "github.com/ChaosHour/mycdc/internal/source/mysql"
	"github.com/ChaosHour/mycdc/internal/state"
)

func newRunCmd() *cobra.Command {
	var configPath string

	c := &cobra.Command{
		Use:   "run",
		Short: "Start capturing changes for a pipeline",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}

			logger := newLogger(cfg.LogLevel)

			tableFilter, err := filter.New(cfg.Tables.Include, cfg.Tables.Exclude)
			if err != nil {
				return err
			}

			snk, err := sink.NewAll(cfg.Sinks)
			if err != nil {
				return err
			}
			defer snk.Close()

			src, err := mysqlsource.New(cfg.MySQL)
			if err != nil {
				return err
			}
			defer src.Close()

			p, err := pipeline.New(pipeline.Options{
				Name:          cfg.Pipeline,
				Source:        src,
				Sink:          snk,
				State:         state.NewFileStore(cfg.State.Path),
				Filter:        tableFilter,
				FlushInterval: cfg.State.FlushInterval.Duration,
				Logger:        logger,
			})
			if err != nil {
				return err
			}
			defer p.Close()

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			runErr := p.Run(ctx)
			if runErr != nil && ctx.Err() != nil {
				// Cancelled by an interrupt/terminate signal: an orderly
				// shutdown, not a failure.
				logger.Info("shutdown complete")
				return nil
			}
			return runErr
		},
	}

	c.Flags().StringVarP(&configPath, "config", "c", "", "path to the pipeline config file (YAML or JSON)")
	_ = c.MarkFlagRequired("config")
	return c
}

// newLogger builds a structured logger writing to stderr, so log output
// never interleaves with event data written to stdout by the stdout sink.
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}
