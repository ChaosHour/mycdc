package filter

import "testing"

func TestNewInvalidPattern(t *testing.T) {
	if _, err := New([]string{"shop.["}, nil); err == nil {
		t.Fatal("expected error for malformed include pattern")
	}
	if _, err := New(nil, []string{"shop.["}); err == nil {
		t.Fatal("expected error for malformed exclude pattern")
	}
}

func TestAllowsNoPatternsAllowsEverything(t *testing.T) {
	f, err := New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Allows("shop", "orders") {
		t.Error("expected table to be allowed with no filters configured")
	}
}

func TestAllowsExcludeWins(t *testing.T) {
	f, err := New([]string{"shop.*"}, []string{"shop.audit_log"})
	if err != nil {
		t.Fatal(err)
	}
	if f.Allows("shop", "audit_log") {
		t.Error("exclude should reject shop.audit_log even though it matches include")
	}
	if !f.Allows("shop", "orders") {
		t.Error("shop.orders should be allowed")
	}
}

func TestAllowsIncludeRestricts(t *testing.T) {
	f, err := New([]string{"shop.orders", "shop.order_items"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]bool{
		"orders":      true,
		"order_items": true,
		"customers":   false,
	}
	for table, want := range cases {
		if got := f.Allows("shop", table); got != want {
			t.Errorf("Allows(shop, %q) = %v, want %v", table, got, want)
		}
	}
}

func TestAllowsWildcardDatabase(t *testing.T) {
	f, err := New([]string{"*.audit_*"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Allows("shop", "audit_log") {
		t.Error("expected *.audit_* to match shop.audit_log")
	}
	if !f.Allows("billing", "audit_events") {
		t.Error("expected *.audit_* to match billing.audit_events")
	}
	if f.Allows("shop", "orders") {
		t.Error("did not expect *.audit_* to match shop.orders")
	}
}

func TestAllowsExactDatabaseWildcardNotCrossDatabase(t *testing.T) {
	f, err := New([]string{"shop.*"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f.Allows("billing", "orders") {
		t.Error("shop.* must not match billing.orders")
	}
}

func TestAllowsEmptyIncludeWithExcludeOnly(t *testing.T) {
	f, err := New(nil, []string{"shop.tmp_*"})
	if err != nil {
		t.Fatal(err)
	}
	if f.Allows("shop", "tmp_scratch") {
		t.Error("shop.tmp_scratch should be excluded")
	}
	if !f.Allows("shop", "orders") {
		t.Error("shop.orders should still be allowed")
	}
}
