package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ChaosHour/mycdc/internal/config"
)

func newValidateCmd() *cobra.Command {
	var configPath string

	c := &cobra.Command{
		Use:   "validate",
		Short: "Validate a pipeline config file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s is valid\n", configPath)
			fmt.Fprintf(cmd.OutOrStdout(), "  pipeline: %s\n", cfg.Pipeline)
			fmt.Fprintf(cmd.OutOrStdout(), "  source:   %s:%d (server_id %d)\n", cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.ServerID)
			fmt.Fprintf(cmd.OutOrStdout(), "  sinks:    %d\n", len(cfg.Sinks))
			for _, s := range cfg.Sinks {
				fmt.Fprintf(cmd.OutOrStdout(), "    - %s\n", s.Type)
			}
			return nil
		},
	}

	c.Flags().StringVarP(&configPath, "config", "c", "", "path to the pipeline config file (YAML or JSON)")
	_ = c.MarkFlagRequired("config")
	return c
}
