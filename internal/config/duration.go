package config

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from either a Go duration
// string ("30s", "5m") or a raw number of nanoseconds, in both YAML and
// JSON config files.
type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	dur, err := parseDurationValue(raw)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}

func (d Duration) MarshalYAML() (any, error) {
	return d.Duration.String(), nil
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var raw any
	if err := value.Decode(&raw); err != nil {
		return err
	}
	dur, err := parseDurationValue(raw)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}

func parseDurationValue(raw any) (time.Duration, error) {
	switch v := raw.(type) {
	case string:
		dur, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", v, err)
		}
		return dur, nil
	case float64:
		return time.Duration(v), nil
	case int:
		return time.Duration(v), nil
	case int64:
		return time.Duration(v), nil
	default:
		return 0, fmt.Errorf("invalid duration value: %v (%T)", raw, raw)
	}
}
