// Package filter decides which tables a pipeline should capture changes
// from, based on glob-style include/exclude patterns.
package filter

import (
	"fmt"
	"path"
)

// TableFilter matches "database.table" identifiers against glob patterns
// such as "shop.orders", "shop.*", or "*.audit_*".
//
// Rules:
//   - If Exclude matches, the table is rejected, regardless of Include.
//   - If Include is empty, every table not excluded is allowed.
//   - If Include is non-empty, a table must match at least one Include
//     pattern (and no Exclude pattern) to be allowed.
type TableFilter struct {
	include []string
	exclude []string
}

// New builds a TableFilter, validating that every pattern is well-formed.
func New(include, exclude []string) (*TableFilter, error) {
	for _, p := range include {
		if err := checkPattern(p); err != nil {
			return nil, fmt.Errorf("filter: include pattern %q: %w", p, err)
		}
	}
	for _, p := range exclude {
		if err := checkPattern(p); err != nil {
			return nil, fmt.Errorf("filter: exclude pattern %q: %w", p, err)
		}
	}
	return &TableFilter{include: include, exclude: exclude}, nil
}

func checkPattern(p string) error {
	_, err := path.Match(p, "")
	return err
}

// Allows reports whether changes on the given database.table should be
// captured.
func (f *TableFilter) Allows(database, table string) bool {
	qualified := database + "." + table

	for _, p := range f.exclude {
		if matches(p, qualified) {
			return false
		}
	}
	if len(f.include) == 0 {
		return true
	}
	for _, p := range f.include {
		if matches(p, qualified) {
			return true
		}
	}
	return false
}

func matches(pattern, qualified string) bool {
	ok, err := path.Match(pattern, qualified)
	return err == nil && ok
}
