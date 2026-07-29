// Package config loads and validates mycdc pipeline configuration from
// either YAML or JSON files, chosen automatically by file extension.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// Config is the root of a pipeline configuration file.
type Config struct {
	// Pipeline is a short, unique name for this capture pipeline. It is
	// stamped onto every emitted event and used as the state store's key,
	// so it must be stable across restarts.
	Pipeline string `json:"pipeline" yaml:"pipeline"`

	MySQL  MySQLSource  `json:"mysql" yaml:"mysql"`
	Tables TableFilter  `json:"tables" yaml:"tables"`
	State  StateConfig  `json:"state" yaml:"state"`
	Sinks  []SinkConfig `json:"sinks" yaml:"sinks"`

	LogLevel string `json:"log_level,omitempty" yaml:"log_level,omitempty"`
}

// MySQLSource configures the binlog replication connection.
type MySQLSource struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`

	// ServerID must be unique among all replication clients (including
	// real replicas) connected to the source server. There is no safe
	// default, so it is required.
	ServerID uint32 `json:"server_id" yaml:"server_id"`

	// Flavor is "mysql" (default) or "mariadb". mycdc targets MySQL
	// 5.7 through 9.x; MariaDB is accepted because the binlog protocol is
	// compatible, but is not part of the tested support matrix.
	Flavor string `json:"flavor,omitempty" yaml:"flavor,omitempty"`

	Charset         string   `json:"charset,omitempty" yaml:"charset,omitempty"`
	HeartbeatPeriod Duration `json:"heartbeat_period" yaml:"heartbeat_period"`
	ReadTimeout     Duration `json:"read_timeout" yaml:"read_timeout"`
}

// TableFilter selects which tables produce events. Patterns are glob-style
// "database.table" strings (e.g. "shop.orders", "shop.*", "*.audit_*").
// Exclude always wins over Include when both match a table.
type TableFilter struct {
	Include []string `json:"include,omitempty" yaml:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty" yaml:"exclude,omitempty"`
}

// StateConfig configures where replication position checkpoints are
// persisted between runs.
type StateConfig struct {
	// Type is currently always "file".
	Type          string   `json:"type" yaml:"type"`
	Path          string   `json:"path" yaml:"path"`
	FlushInterval Duration `json:"flush_interval" yaml:"flush_interval"`
}

// SinkConfig configures one destination for captured events. A pipeline may
// fan out to multiple sinks.
type SinkConfig struct {
	// Type is one of "stdout", "file", "webhook".
	Type string `json:"type" yaml:"type"`

	// Format applies to the stdout sink: "json" (default) or "pretty".
	Format string `json:"format,omitempty" yaml:"format,omitempty"`

	// Path applies to the file sink: the JSON-lines file to append to.
	Path string `json:"path,omitempty" yaml:"path,omitempty"`

	// URL, Headers, Timeout, MaxRetries and RetryBackoff apply to the
	// webhook sink.
	URL          string            `json:"url,omitempty" yaml:"url,omitempty"`
	Headers      map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
	Timeout      Duration          `json:"timeout" yaml:"timeout"`
	MaxRetries   int               `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
	RetryBackoff Duration          `json:"retry_backoff" yaml:"retry_backoff"`
}

// Load reads and parses a config file, choosing the format (YAML or JSON)
// from its extension, applying defaults, expanding ${ENV_VAR} references
// in string fields, and validating the result.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}

	cfg, err := Parse(data, formatFor(path))
	if err != nil {
		return nil, fmt.Errorf("config: parsing %s: %w", path, err)
	}
	return cfg, nil
}

// Format identifies a config file's serialization.
type Format string

const (
	FormatYAML Format = "yaml"
	FormatJSON Format = "json"
)

func formatFor(path string) Format {
	if len(path) >= 5 && path[len(path)-5:] == ".json" {
		return FormatJSON
	}
	return FormatYAML
}

// Parse decodes raw config bytes in the given format, applies defaults,
// expands environment variables, and validates the result.
func Parse(data []byte, format Format) (*Config, error) {
	var cfg Config
	switch format {
	case FormatJSON:
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("invalid JSON: %w", err)
		}
	case FormatYAML:
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config format %q", format)
	}

	cfg.expandEnv()
	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnvString replaces ${VAR} references with the value of the VAR
// environment variable. A reference to an unset variable is left
// unexpanded (rather than silently becoming an empty string) so
// misconfiguration is easy to spot.
func expandEnvString(s string) string {
	return envRef.ReplaceAllStringFunc(s, func(match string) string {
		name := envRef.FindStringSubmatch(match)[1]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		return match
	})
}

func (c *Config) expandEnv() {
	c.MySQL.Password = expandEnvString(c.MySQL.Password)
	for i := range c.Sinks {
		c.Sinks[i].URL = expandEnvString(c.Sinks[i].URL)
		for k, v := range c.Sinks[i].Headers {
			c.Sinks[i].Headers[k] = expandEnvString(v)
		}
	}
}

func (c *Config) applyDefaults() {
	if c.MySQL.Flavor == "" {
		c.MySQL.Flavor = "mysql"
	}
	if c.MySQL.Charset == "" {
		c.MySQL.Charset = "utf8mb4"
	}
	if c.MySQL.HeartbeatPeriod.Duration == 0 {
		c.MySQL.HeartbeatPeriod.Duration = defaultHeartbeatPeriod
	}
	if c.MySQL.ReadTimeout.Duration == 0 {
		c.MySQL.ReadTimeout.Duration = defaultReadTimeout
	}
	if c.State.Type == "" {
		c.State.Type = "file"
	}
	if c.State.FlushInterval.Duration == 0 {
		c.State.FlushInterval.Duration = defaultFlushInterval
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	for i := range c.Sinks {
		s := &c.Sinks[i]
		switch s.Type {
		case "stdout":
			if s.Format == "" {
				s.Format = "json"
			}
		case "webhook":
			if s.Timeout.Duration == 0 {
				s.Timeout.Duration = defaultWebhookTimeout
			}
			if s.MaxRetries == 0 {
				s.MaxRetries = defaultWebhookMaxRetries
			}
			if s.RetryBackoff.Duration == 0 {
				s.RetryBackoff.Duration = defaultWebhookRetryBackoff
			}
		}
	}
}
