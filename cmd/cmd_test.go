package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ChaosHour/mycdc/internal/config"
	"github.com/ChaosHour/mycdc/internal/event"
	"github.com/ChaosHour/mycdc/internal/state"
)

// execute runs the root command with args, capturing combined stdout output,
// and returns it along with any error.
func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

func TestRootCmdHasAllSubcommands(t *testing.T) {
	root := NewRootCmd()
	want := []string{"init", "validate", "run", "status", "version"}
	for _, name := range want {
		found := false
		for _, c := range root.Commands() {
			if c.Name() == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("root command missing subcommand %q", name)
		}
	}
}

func TestVersionCmd(t *testing.T) {
	out, err := execute(t, "version")
	if err != nil {
		t.Fatalf("version command error: %v", err)
	}
	if !strings.Contains(out, "mycdc") {
		t.Errorf("output = %q, want it to contain \"mycdc\"", out)
	}
}

func TestInitCmdWritesValidYAML(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "mycdc.yaml")

	out, err := execute(t, "init", "--format", "yaml", "--output", output)
	if err != nil {
		t.Fatalf("init error: %v\noutput: %s", err, out)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatalf("expected %s to exist: %v", output, err)
	}

	t.Setenv("MYCDC_MYSQL_PASSWORD", "s3cret")
	if _, err := config.Load(output); err != nil {
		t.Errorf("generated config should be valid, got error: %v", err)
	}
}

func TestInitCmdWritesValidJSON(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "mycdc.json")

	if _, err := execute(t, "init", "--format", "json", "--output", output); err != nil {
		t.Fatalf("init error: %v", err)
	}

	t.Setenv("MYCDC_MYSQL_PASSWORD", "s3cret")
	if _, err := config.Load(output); err != nil {
		t.Errorf("generated config should be valid, got error: %v", err)
	}
}

func TestInitCmdRefusesToOverwriteWithoutForce(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "mycdc.yaml")

	if _, err := execute(t, "init", "--output", output); err != nil {
		t.Fatalf("first init error: %v", err)
	}
	if _, err := execute(t, "init", "--output", output); err == nil {
		t.Fatal("expected second init without --force to fail")
	}
}

func TestInitCmdForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "mycdc.yaml")

	if _, err := execute(t, "init", "--output", output); err != nil {
		t.Fatalf("first init error: %v", err)
	}
	if _, err := execute(t, "init", "--output", output, "--force"); err != nil {
		t.Fatalf("forced init error: %v", err)
	}
}

func TestInitCmdRejectsUnknownFormat(t *testing.T) {
	dir := t.TempDir()
	if _, err := execute(t, "init", "--format", "toml", "--output", filepath.Join(dir, "x")); err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func validConfigYAML(statePath string) string {
	return `
pipeline: test-pipeline
mysql:
  host: 127.0.0.1
  port: 3306
  user: repl
  server_id: 1001
state:
  path: ` + statePath + `
sinks:
  - type: stdout
`
}

func TestValidateCmdValidConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "mycdc.yaml")
	if err := os.WriteFile(cfgPath, []byte(validConfigYAML(filepath.Join(dir, "state.json"))), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err := execute(t, "validate", "-c", cfgPath)
	if err != nil {
		t.Fatalf("validate error: %v\noutput: %s", err, out)
	}
	if !strings.Contains(out, "is valid") || !strings.Contains(out, "test-pipeline") {
		t.Errorf("output = %q", out)
	}
}

func TestValidateCmdInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(cfgPath, []byte("pipeline: p\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := execute(t, "validate", "-c", cfgPath); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateCmdRequiresConfigFlag(t *testing.T) {
	if _, err := execute(t, "validate"); err == nil {
		t.Fatal("expected error for missing required --config flag")
	}
}

func TestValidateCmdMissingFile(t *testing.T) {
	if _, err := execute(t, "validate", "-c", "/nonexistent/mycdc.yaml"); err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestStatusCmdNoCheckpointYet(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	cfgPath := filepath.Join(dir, "mycdc.yaml")
	if err := os.WriteFile(cfgPath, []byte(validConfigYAML(statePath)), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err := execute(t, "status", "-c", cfgPath)
	if err != nil {
		t.Fatalf("status error: %v", err)
	}
	if !strings.Contains(out, "no checkpoint yet") {
		t.Errorf("output = %q, want it to mention no checkpoint yet", out)
	}
}

func TestStatusCmdWithCheckpoint(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	cfgPath := filepath.Join(dir, "mycdc.yaml")
	if err := os.WriteFile(cfgPath, []byte(validConfigYAML(statePath)), 0o644); err != nil {
		t.Fatal(err)
	}

	st := state.NewFileStore(statePath)
	if err := st.Save("test-pipeline", event.Position{File: "binlog.000005", Pos: 4242}); err != nil {
		t.Fatal(err)
	}

	out, err := execute(t, "status", "-c", cfgPath)
	if err != nil {
		t.Fatalf("status error: %v", err)
	}
	if !strings.Contains(out, "binlog.000005:4242") {
		t.Errorf("output = %q, want it to contain the checkpoint position", out)
	}
}

func TestStatusCmdRequiresConfigFlag(t *testing.T) {
	if _, err := execute(t, "status"); err == nil {
		t.Fatal("expected error for missing required --config flag")
	}
}

func TestRunCmdRequiresConfigFlag(t *testing.T) {
	if _, err := execute(t, "run"); err == nil {
		t.Fatal("expected error for missing required --config flag")
	}
}

func TestRunCmdFailsFastOnMissingConfigFile(t *testing.T) {
	// Should fail at config.Load, before ever attempting a MySQL
	// connection.
	if _, err := execute(t, "run", "-c", "/nonexistent/mycdc.yaml"); err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestRunCmdFailsFastOnInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.yaml")
	// Missing required mysql fields and sinks.
	if err := os.WriteFile(cfgPath, []byte("pipeline: p\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := execute(t, "run", "-c", cfgPath); err == nil {
		t.Fatal("expected validation error before attempting to connect")
	}
}
