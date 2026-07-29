package sink

import "encoding/json"

// marshalRow renders a row's column values as compact JSON for the
// "pretty" stdout format.
func marshalRow(row map[string]any) ([]byte, error) {
	return json.Marshal(row)
}
