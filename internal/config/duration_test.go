package config

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type durationHolder struct {
	D Duration `json:"d" yaml:"d"`
}

func TestDurationUnmarshalJSONString(t *testing.T) {
	var h durationHolder
	if err := json.Unmarshal([]byte(`{"d":"1h30m"}`), &h); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if h.D.Duration != 90*time.Minute {
		t.Errorf("D = %v, want 90m", h.D.Duration)
	}
}

func TestDurationUnmarshalJSONNumber(t *testing.T) {
	var h durationHolder
	if err := json.Unmarshal([]byte(`{"d":1000000000}`), &h); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if h.D.Duration != time.Second {
		t.Errorf("D = %v, want 1s", h.D.Duration)
	}
}

func TestDurationUnmarshalJSONInvalid(t *testing.T) {
	var h durationHolder
	if err := json.Unmarshal([]byte(`{"d":"not-a-duration"}`), &h); err == nil {
		t.Fatal("expected error for invalid duration string")
	}
	if err := json.Unmarshal([]byte(`{"d":true}`), &h); err == nil {
		t.Fatal("expected error for non-string/number duration")
	}
}

func TestDurationUnmarshalYAML(t *testing.T) {
	var h durationHolder
	if err := yaml.Unmarshal([]byte("d: 45s\n"), &h); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if h.D.Duration != 45*time.Second {
		t.Errorf("D = %v, want 45s", h.D.Duration)
	}
}

func TestDurationUnmarshalYAMLInvalid(t *testing.T) {
	var h durationHolder
	if err := yaml.Unmarshal([]byte("d: not-a-duration\n"), &h); err == nil {
		t.Fatal("expected error for invalid duration string")
	}
}

func TestDurationMarshalJSONRoundTrip(t *testing.T) {
	h := durationHolder{D: Duration{Duration: 2 * time.Minute}}
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var back durationHolder
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if back.D.Duration != h.D.Duration {
		t.Errorf("round trip = %v, want %v", back.D.Duration, h.D.Duration)
	}
}

func TestDurationMarshalYAMLRoundTrip(t *testing.T) {
	h := durationHolder{D: Duration{Duration: 90 * time.Second}}
	b, err := yaml.Marshal(h)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	var back durationHolder
	if err := yaml.Unmarshal(b, &back); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if back.D.Duration != h.D.Duration {
		t.Errorf("round trip = %v, want %v", back.D.Duration, h.D.Duration)
	}
}
