package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const exampleYAML = `# mycdc pipeline configuration.
# Run "mycdc validate -c mycdc.yaml" after filling this in, then
# "mycdc run -c mycdc.yaml" to start capturing.

# Unique, stable name for this pipeline. Used as the checkpoint key in the
# state file and stamped onto every emitted event.
pipeline: my-pipeline

mysql:
  host: 127.0.0.1
  port: 3306
  user: mycdc_repl
  # Prefer an environment variable over a literal password in the file.
  # ${VAR} is expanded from the environment at load time.
  password: ${MYCDC_MYSQL_PASSWORD}
  # Must be unique among every replication client (including real
  # replicas) connected to this MySQL server. There is no safe default.
  server_id: 1001
  # flavor: mysql        # or "mariadb"; mycdc targets MySQL 5.7-9.x
  # charset: utf8mb4
  # heartbeat_period: 30s
  # read_timeout: 90s

# Which tables to capture. Patterns are glob-style "database.table"
# (e.g. "shop.orders", "shop.*", "*.audit_*"). Exclude always wins.
# Leave both empty to capture every table.
tables:
  include:
    - "shop.orders"
    - "shop.order_items"
  exclude:
    - "shop.audit_log"

# Where replication position checkpoints are persisted between runs.
state:
  path: ./state/my-pipeline.json
  # flush_interval: 5s

# One or more destinations for captured events.
sinks:
  - type: stdout
    format: json          # or "pretty"

  # - type: file
  #   path: ./out/my-pipeline.jsonl

  # - type: webhook
  #   url: https://example.com/cdc-events
  #   headers:
  #     Authorization: "Bearer ${MYCDC_WEBHOOK_TOKEN}"
  #   timeout: 10s
  #   max_retries: 3
  #   retry_backoff: 500ms

# log_level: info   # debug | info | warn | error
`

const exampleJSON = `{
  "pipeline": "my-pipeline",
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "mycdc_repl",
    "password": "${MYCDC_MYSQL_PASSWORD}",
    "server_id": 1001
  },
  "tables": {
    "include": ["shop.orders", "shop.order_items"],
    "exclude": ["shop.audit_log"]
  },
  "state": {
    "path": "./state/my-pipeline.json"
  },
  "sinks": [
    {"type": "stdout", "format": "json"}
  ]
}
`

func newInitCmd() *cobra.Command {
	var format, output string
	var force bool

	c := &cobra.Command{
		Use:   "init",
		Short: "Scaffold a new pipeline config file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var content string
			switch format {
			case "yaml":
				content = exampleYAML
				if output == "" {
					output = "mycdc.yaml"
				}
			case "json":
				content = exampleJSON
				if output == "" {
					output = "mycdc.json"
				}
			default:
				return fmt.Errorf("--format must be \"yaml\" or \"json\", got %q", format)
			}

			if !force {
				if _, err := os.Stat(output); err == nil {
					return fmt.Errorf("%s already exists; use --force to overwrite", output)
				}
			}

			if err := os.WriteFile(output, []byte(content), 0o644); err != nil {
				return fmt.Errorf("writing %s: %w", output, err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "wrote %s\n", output)
			return nil
		},
	}

	c.Flags().StringVar(&format, "format", "yaml", `config format to generate: "yaml" or "json"`)
	c.Flags().StringVarP(&output, "output", "o", "", "output path (default: mycdc.yaml or mycdc.json)")
	c.Flags().BoolVar(&force, "force", false, "overwrite the output file if it already exists")
	return c
}
