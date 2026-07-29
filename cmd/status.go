package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ChaosHour/mycdc/internal/config"
	"github.com/ChaosHour/mycdc/internal/state"
)

func newStatusCmd() *cobra.Command {
	var configPath string

	c := &cobra.Command{
		Use:   "status",
		Short: "Show the last saved checkpoint for a pipeline",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}

			st := state.NewFileStore(cfg.State.Path)
			pos, err := st.Load(cfg.Pipeline)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "pipeline:   %s\n", cfg.Pipeline)
			fmt.Fprintf(out, "state file: %s\n", cfg.State.Path)
			if pos.IsZero() {
				fmt.Fprintln(out, "position:   <no checkpoint yet -- next run starts from the current binlog position>")
			} else {
				fmt.Fprintf(out, "position:   %s\n", pos.String())
			}
			return nil
		},
	}

	c.Flags().StringVarP(&configPath, "config", "c", "", "path to the pipeline config file (YAML or JSON)")
	_ = c.MarkFlagRequired("config")
	return c
}
