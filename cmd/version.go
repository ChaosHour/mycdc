package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Version is stamped at build time via:
//
//	go build -ldflags "-X github.com/ChaosHour/mycdc/cmd.Version=1.2.3"
var Version = "dev"

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the mycdc version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprintf(cmd.OutOrStdout(), "mycdc %s\n", Version)
			return nil
		},
	}
}
