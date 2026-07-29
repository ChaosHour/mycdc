// Command mycdc captures row-level changes from MySQL via binlog
// replication and delivers them to configurable sinks.
package main

import (
	"os"

	"github.com/ChaosHour/mycdc/cmd"
)

func main() {
	if err := cmd.NewRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
