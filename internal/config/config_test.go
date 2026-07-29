package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const yamlFixture = `
pipeline: orders-cdc
mysql:
  host: 127.0.0.1
  port: 3306
  user: repl
  password: ${MYCDC_TEST_PASSWORD}
  server_id: 1001
tables:
  include:
    - "shop.orders"
    - "shop.order_items"
  exclude:
    - "shop.audit_log"
state:
  path: ./state/orders-cdc.json
sinks:
  - type: stdout
  - type: file
    path: ./out/orders-cdc.jsonl
  - type: webhook
    url: https://example.com/cdc
    headers:
      Authorization: "Bearer ${MYCDC_TEST_TOKEN}"
`

const jsonFixture = `
{
  "pipeline": "orders-cdc",
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "repl",
    "password": "${MYCDC_TEST_PASSWORD}",
    "server_id": 1001
  },
  "tables": {
    "include": ["shop.orders", "shop.order_items"],
    "exclude": ["shop.audit_log"]
  },
  "state": {
    "path": "./state/orders-cdc.json"
  },
  "sinks": [
    {"type": "stdout"},
    {"type": "file", "path": "./out/orders-cdc.jsonl"},
    {"type": "webhook", "url": "https://example.com/cdc", "headers": {"Authorization": "Bearer ${MYCDC_TEST_TOKEN}"}}
  ]
}
`

func assertParsedFixture(t *testing.T, cfg *Config) {
	t.Helper()

	if cfg.Pipeline != "orders-cdc" {
		t.Errorf("Pipeline = %q, want orders-cdc", cfg.Pipeline)
	}
	if cfg.MySQL.Host != "127.0.0.1" || cfg.MySQL.Port != 3306 || cfg.MySQL.User != "repl" {
		t.Errorf("MySQL connection fields wrong: %+v", cfg.MySQL)
	}
	if cfg.MySQL.Password != "s3cret" {
		t.Errorf("Password = %q, want env-expanded s3cret", cfg.MySQL.Password)
	}
	if cfg.MySQL.ServerID != 1001 {
		t.Errorf("ServerID = %d, want 1001", cfg.MySQL.ServerID)
	}
	// Defaults.
	if cfg.MySQL.Flavor != "mysql" {
		t.Errorf("Flavor default = %q, want mysql", cfg.MySQL.Flavor)
	}
	if cfg.MySQL.Charset != "utf8mb4" {
		t.Errorf("Charset default = %q, want utf8mb4", cfg.MySQL.Charset)
	}
	if cfg.MySQL.HeartbeatPeriod.Duration != defaultHeartbeatPeriod {
		t.Errorf("HeartbeatPeriod default = %v, want %v", cfg.MySQL.HeartbeatPeriod.Duration, defaultHeartbeatPeriod)
	}
	if cfg.State.FlushInterval.Duration != defaultFlushInterval {
		t.Errorf("State.FlushInterval default = %v, want %v", cfg.State.FlushInterval.Duration, defaultFlushInterval)
	}

	if len(cfg.Tables.Include) != 2 || cfg.Tables.Include[0] != "shop.orders" {
		t.Errorf("Tables.Include = %v", cfg.Tables.Include)
	}
	if len(cfg.Tables.Exclude) != 1 || cfg.Tables.Exclude[0] != "shop.audit_log" {
		t.Errorf("Tables.Exclude = %v", cfg.Tables.Exclude)
	}

	if len(cfg.Sinks) != 3 {
		t.Fatalf("Sinks = %d, want 3", len(cfg.Sinks))
	}
	if cfg.Sinks[0].Type != "stdout" || cfg.Sinks[0].Format != "json" {
		t.Errorf("stdout sink defaults wrong: %+v", cfg.Sinks[0])
	}
	if cfg.Sinks[1].Type != "file" || cfg.Sinks[1].Path != "./out/orders-cdc.jsonl" {
		t.Errorf("file sink wrong: %+v", cfg.Sinks[1])
	}
	webhook := cfg.Sinks[2]
	if webhook.Type != "webhook" || webhook.URL != "https://example.com/cdc" {
		t.Errorf("webhook sink wrong: %+v", webhook)
	}
	if webhook.Headers["Authorization"] != "Bearer tok123" {
		t.Errorf("webhook header not env-expanded: %+v", webhook.Headers)
	}
	if webhook.Timeout.Duration != defaultWebhookTimeout {
		t.Errorf("webhook Timeout default = %v, want %v", webhook.Timeout.Duration, defaultWebhookTimeout)
	}
	if webhook.MaxRetries != defaultWebhookMaxRetries {
		t.Errorf("webhook MaxRetries default = %d, want %d", webhook.MaxRetries, defaultWebhookMaxRetries)
	}
	if webhook.RetryBackoff.Duration != defaultWebhookRetryBackoff {
		t.Errorf("webhook RetryBackoff default = %v, want %v", webhook.RetryBackoff.Duration, defaultWebhookRetryBackoff)
	}
}

func TestParseYAMLAndJSONAreEquivalent(t *testing.T) {
	t.Setenv("MYCDC_TEST_PASSWORD", "s3cret")
	t.Setenv("MYCDC_TEST_TOKEN", "tok123")

	yamlCfg, err := Parse([]byte(yamlFixture), FormatYAML)
	if err != nil {
		t.Fatalf("Parse(yaml) error: %v", err)
	}
	assertParsedFixture(t, yamlCfg)

	jsonCfg, err := Parse([]byte(jsonFixture), FormatJSON)
	if err != nil {
		t.Fatalf("Parse(json) error: %v", err)
	}
	assertParsedFixture(t, jsonCfg)
}

func TestLoadDetectsFormatByExtension(t *testing.T) {
	t.Setenv("MYCDC_TEST_PASSWORD", "s3cret")
	t.Setenv("MYCDC_TEST_TOKEN", "tok123")

	dir := t.TempDir()

	yamlPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(yamlPath, []byte(yamlFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	if cfg, err := Load(yamlPath); err != nil {
		t.Fatalf("Load(.yaml) error: %v", err)
	} else {
		assertParsedFixture(t, cfg)
	}

	ymlPath := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(ymlPath, []byte(yamlFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(ymlPath); err != nil {
		t.Fatalf("Load(.yml) error: %v", err)
	}

	jsonPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(jsonPath, []byte(jsonFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	if cfg, err := Load(jsonPath); err != nil {
		t.Fatalf("Load(.json) error: %v", err)
	} else {
		assertParsedFixture(t, cfg)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestExpandEnvLeavesUnsetVarsUnexpanded(t *testing.T) {
	os.Unsetenv("MYCDC_DEFINITELY_UNSET")
	cfg, err := Parse([]byte(`
pipeline: p
mysql: {host: h, port: 3306, user: u, server_id: 1, password: "${MYCDC_DEFINITELY_UNSET}"}
state: {path: ./state.json}
sinks: [{type: stdout}]
`), FormatYAML)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if cfg.MySQL.Password != "${MYCDC_DEFINITELY_UNSET}" {
		t.Errorf("Password = %q, want unexpanded placeholder", cfg.MySQL.Password)
	}
}

func TestDurationOverride(t *testing.T) {
	cfg, err := Parse([]byte(`
pipeline: p
mysql: {host: h, port: 3306, user: u, server_id: 1, heartbeat_period: "45s", read_timeout: "2m"}
state: {path: ./state.json, flush_interval: "1s"}
sinks: [{type: webhook, url: "https://x", timeout: "3s", max_retries: 7, retry_backoff: "250ms"}]
`), FormatYAML)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if cfg.MySQL.HeartbeatPeriod.Duration != 45*time.Second {
		t.Errorf("HeartbeatPeriod = %v, want 45s", cfg.MySQL.HeartbeatPeriod.Duration)
	}
	if cfg.MySQL.ReadTimeout.Duration != 2*time.Minute {
		t.Errorf("ReadTimeout = %v, want 2m", cfg.MySQL.ReadTimeout.Duration)
	}
	if cfg.State.FlushInterval.Duration != time.Second {
		t.Errorf("FlushInterval = %v, want 1s", cfg.State.FlushInterval.Duration)
	}
	if cfg.Sinks[0].Timeout.Duration != 3*time.Second {
		t.Errorf("webhook Timeout = %v, want 3s", cfg.Sinks[0].Timeout.Duration)
	}
	if cfg.Sinks[0].MaxRetries != 7 {
		t.Errorf("webhook MaxRetries = %d, want 7", cfg.Sinks[0].MaxRetries)
	}
	if cfg.Sinks[0].RetryBackoff.Duration != 250*time.Millisecond {
		t.Errorf("webhook RetryBackoff = %v, want 250ms", cfg.Sinks[0].RetryBackoff.Duration)
	}
}

func TestParseInvalidSyntax(t *testing.T) {
	if _, err := Parse([]byte("not: valid: yaml: at all: ["), FormatYAML); err == nil {
		t.Error("expected error for malformed YAML")
	}
	if _, err := Parse([]byte("{not valid json"), FormatJSON); err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr []string
	}{
		{
			name: "missing pipeline name",
			yaml: `mysql: {host: h, port: 3306, user: u, server_id: 1}
state: {path: s}
sinks: [{type: stdout}]`,
			wantErr: []string{"pipeline: name is required"},
		},
		{
			name: "invalid pipeline name",
			yaml: `pipeline: "bad name!"
mysql: {host: h, port: 3306, user: u, server_id: 1}
state: {path: s}
sinks: [{type: stdout}]`,
			wantErr: []string{"pipeline:"},
		},
		{
			name: "missing mysql fields",
			yaml: `pipeline: p
mysql: {port: 999999}
state: {path: s}
sinks: [{type: stdout}]`,
			wantErr: []string{"mysql.host", "mysql.user", "mysql.server_id", "mysql.port"},
		},
		{
			name: "missing sinks",
			yaml: `pipeline: p
mysql: {host: h, port: 3306, user: u, server_id: 1}
state: {path: s}`,
			wantErr: []string{"sinks: at least one sink is required"},
		},
		{
			name: "unknown sink type",
			yaml: `pipeline: p
mysql: {host: h, port: 3306, user: u, server_id: 1}
state: {path: s}
sinks: [{type: carrier-pigeon}]`,
			wantErr: []string{"sinks[0].type"},
		},
		{
			name: "file sink missing path",
			yaml: `pipeline: p
mysql: {host: h, port: 3306, user: u, server_id: 1}
state: {path: s}
sinks: [{type: file}]`,
			wantErr: []string{"sinks[0].path"},
		},
		{
			name: "webhook sink missing url",
			yaml: `pipeline: p
mysql: {host: h, port: 3306, user: u, server_id: 1}
state: {path: s}
sinks: [{type: webhook}]`,
			wantErr: []string{"sinks[0].url"},
		},
		{
			name: "missing state path",
			yaml: `pipeline: p
mysql: {host: h, port: 3306, user: u, server_id: 1}
state: {}
sinks: [{type: stdout}]`,
			wantErr: []string{"state.path"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.yaml), FormatYAML)
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			for _, want := range c.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

func TestValidationErrorSingular(t *testing.T) {
	_, err := Parse([]byte(`
pipeline: p
mysql: {host: h, port: 3306, user: u, server_id: 1}
state: {path: s}
`), FormatYAML)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "sinks: at least one sink is required" {
		t.Errorf("single-error message should not have list formatting, got: %q", err.Error())
	}
}
