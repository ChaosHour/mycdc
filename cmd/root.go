// Package cmd implements the mycdc command-line interface.
package cmd

import "github.com/spf13/cobra"

// NewRootCmd builds the mycdc root command and wires up its subcommands.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "mycdc",
		Short: "Change data capture for MySQL",
		Long: `mycdc captures row-level INSERT/UPDATE/DELETE changes from a MySQL
5.7-9.x server's binlog and delivers them to one or more sinks (stdout, a
local file, or a webhook), checkpointing progress so a restart resumes
instead of re-scanning.`,
		SilenceUsage:  true,
		SilenceErrors: false,
	}

	root.AddCommand(
		newInitCmd(),
		newValidateCmd(),
		newRunCmd(),
		newStatusCmd(),
		newVersionCmd(),
	)
	return root
}
