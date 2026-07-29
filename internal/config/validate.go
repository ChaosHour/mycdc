package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	defaultHeartbeatPeriod     = 30 * time.Second
	defaultReadTimeout         = 90 * time.Second
	defaultFlushInterval       = 5 * time.Second
	defaultWebhookTimeout      = 10 * time.Second
	defaultWebhookMaxRetries   = 3
	defaultWebhookRetryBackoff = 500 * time.Millisecond
)

var pipelineNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)

// Validate checks that the config is complete and internally consistent.
// It reports every problem it finds rather than stopping at the first one,
// so `mycdc validate` can show a user the full list in one pass.
func (c *Config) Validate() error {
	var errs []string

	if c.Pipeline == "" {
		errs = append(errs, "pipeline: name is required")
	} else if !pipelineNamePattern.MatchString(c.Pipeline) {
		errs = append(errs, fmt.Sprintf("pipeline: %q must match %s", c.Pipeline, pipelineNamePattern.String()))
	}

	errs = append(errs, c.MySQL.validate()...)
	errs = append(errs, c.Tables.validate()...)
	errs = append(errs, c.State.validate()...)

	if len(c.Sinks) == 0 {
		errs = append(errs, "sinks: at least one sink is required")
	}
	for i, s := range c.Sinks {
		errs = append(errs, s.validate(i)...)
	}

	if len(errs) == 0 {
		return nil
	}
	return &ValidationError{Errors: errs}
}

func (m MySQLSource) validate() []string {
	var errs []string
	if m.Host == "" {
		errs = append(errs, "mysql.host: is required")
	}
	if m.Port <= 0 || m.Port > 65535 {
		errs = append(errs, fmt.Sprintf("mysql.port: %d is not a valid port", m.Port))
	}
	if m.User == "" {
		errs = append(errs, "mysql.user: is required")
	}
	if m.ServerID == 0 {
		errs = append(errs, "mysql.server_id: is required and must be unique among all replication clients on the source server")
	}
	if m.Flavor != "" && m.Flavor != "mysql" && m.Flavor != "mariadb" {
		errs = append(errs, fmt.Sprintf("mysql.flavor: %q must be \"mysql\" or \"mariadb\"", m.Flavor))
	}
	return errs
}

func (t TableFilter) validate() []string {
	var errs []string
	for _, p := range t.Include {
		if p == "" {
			errs = append(errs, "tables.include: patterns must not be empty")
		}
	}
	for _, p := range t.Exclude {
		if p == "" {
			errs = append(errs, "tables.exclude: patterns must not be empty")
		}
	}
	return errs
}

func (s StateConfig) validate() []string {
	var errs []string
	if s.Type != "" && s.Type != "file" {
		errs = append(errs, fmt.Sprintf("state.type: %q is not supported (only \"file\" is currently implemented)", s.Type))
	}
	if s.Path == "" {
		errs = append(errs, "state.path: is required")
	}
	return errs
}

func (s SinkConfig) validate(index int) []string {
	var errs []string
	prefix := fmt.Sprintf("sinks[%d]", index)
	switch s.Type {
	case "stdout":
		if s.Format != "" && s.Format != "json" && s.Format != "pretty" {
			errs = append(errs, fmt.Sprintf("%s.format: %q must be \"json\" or \"pretty\"", prefix, s.Format))
		}
	case "file":
		if s.Path == "" {
			errs = append(errs, fmt.Sprintf("%s.path: is required for a file sink", prefix))
		}
	case "webhook":
		if s.URL == "" {
			errs = append(errs, fmt.Sprintf("%s.url: is required for a webhook sink", prefix))
		}
		if s.MaxRetries < 0 {
			errs = append(errs, fmt.Sprintf("%s.max_retries: must not be negative", prefix))
		}
	case "":
		errs = append(errs, fmt.Sprintf("%s.type: is required", prefix))
	default:
		errs = append(errs, fmt.Sprintf("%s.type: %q is not a known sink type (want stdout, file, or webhook)", prefix, s.Type))
	}
	return errs
}

// ValidationError collects every problem found while validating a config,
// so callers can print them all at once instead of fixing one at a time.
type ValidationError struct {
	Errors []string
}

func (e *ValidationError) Error() string {
	if len(e.Errors) == 1 {
		return e.Errors[0]
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d configuration errors:", len(e.Errors))
	for _, err := range e.Errors {
		b.WriteString("\n  - ")
		b.WriteString(err)
	}
	return b.String()
}
