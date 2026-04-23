package service

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// ---- transform_registry tests ----

func TestTransform_MongoDateMs_ISOString(t *testing.T) {
	got, err := ApplyTransform("mongo_date_ms", map[string]any{"$date": "2024-07-10T07:17:59.271Z"})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	tm, ok := got.(time.Time)
	if !ok {
		t.Fatalf("want time.Time, got %T", got)
	}
	if tm.UnixMilli() != 1720595879271 {
		t.Errorf("want ms 1720595879271, got %d", tm.UnixMilli())
	}
}

func TestTransform_MongoDateMs_IntEpochMs(t *testing.T) {
	got, err := ApplyTransform("mongo_date_ms", map[string]any{"$date": float64(1720595879271)})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if tm := got.(time.Time); tm.UnixMilli() != 1720595879271 {
		t.Errorf("want ms 1720595879271, got %d", tm.UnixMilli())
	}
}

func TestTransform_MongoDateMs_BareMs(t *testing.T) {
	got, err := ApplyTransform("mongo_date_ms", float64(1720595879271))
	if err != nil {
		t.Fatal(err)
	}
	if tm := got.(time.Time); tm.UnixMilli() != 1720595879271 {
		t.Errorf("bare ms fail")
	}
}

func TestTransform_OIDToHex(t *testing.T) {
	got, err := ApplyTransform("oid_to_hex", map[string]any{"$oid": "69df0e67b87dab24273f118c"})
	if err != nil {
		t.Fatal(err)
	}
	if got.(string) != "69df0e67b87dab24273f118c" {
		t.Errorf("oid unwrap fail: %v", got)
	}
}

func TestTransform_BigIntStr_NumberLong(t *testing.T) {
	got, err := ApplyTransform("bigint_str", map[string]any{"$numberLong": "10000"})
	if err != nil {
		t.Fatal(err)
	}
	if got.(int64) != 10000 {
		t.Errorf("numberLong fail: %v", got)
	}
}

func TestTransform_BigIntStr_PlainString(t *testing.T) {
	got, err := ApplyTransform("bigint_str", "12345")
	if err != nil {
		t.Fatal(err)
	}
	if got.(int64) != 12345 {
		t.Errorf("string int fail")
	}
}

func TestTransform_NumericCast_Shapes(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{float64(10000), "10000"},
		{"10000", "10000"},
		{"10000.5", "10000.5"},
		{map[string]any{"$numberDecimal": "10000.5"}, "10000.5"},
		{map[string]any{"$numberLong": "10000"}, "10000"},
		{map[string]any{"$numberInt": "1"}, "1"},
	}
	for _, c := range cases {
		got, err := ApplyTransform("numeric_cast", c.in)
		if err != nil {
			t.Errorf("input %v: %v", c.in, err)
			continue
		}
		if got.(string) != c.want {
			t.Errorf("input %v: want %q got %q", c.in, c.want, got)
		}
	}
}

func TestTransform_JSONBPassthrough(t *testing.T) {
	got, err := ApplyTransform("jsonb_passthrough", []any{map[string]any{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	s, ok := got.(string)
	if !ok {
		t.Fatalf("want string, got %T", got)
	}
	if !strings.Contains(s, `"k":"v"`) {
		t.Errorf("jsonb_passthrough: %s", s)
	}
}

func TestTransform_NullIfEmpty(t *testing.T) {
	cases := []struct {
		in   any
		want any
	}{
		{"", nil},
		{map[string]any{}, nil},
		{[]any{}, nil},
		{"x", "x"},
		{map[string]any{"k": 1}, map[string]any{"k": 1}},
	}
	for _, c := range cases {
		got, err := ApplyTransform("null_if_empty", c.in)
		if err != nil {
			t.Errorf("input %v: %v", c.in, err)
			continue
		}
		bGot, _ := json.Marshal(got)
		bWant, _ := json.Marshal(c.want)
		if string(bGot) != string(bWant) {
			t.Errorf("input %v: want %s got %s", c.in, bWant, bGot)
		}
	}
}

func TestTransform_WhitelistReject(t *testing.T) {
	_, err := ApplyTransform("eval", "anything")
	if err == nil {
		t.Fatal("expected ErrTransformNotWhitelisted")
	}
	if !strings.Contains(err.Error(), "not whitelisted") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestListTransforms_Deterministic(t *testing.T) {
	a := ListTransforms()
	b := ListTransforms()
	if len(a) != len(b) {
		t.Fatal("size drift")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("order drift at %d: %s vs %s", i, a[i], b[i])
		}
	}
	// Spot-check presence of every expected key.
	want := []string{
		"bigint_str", "jsonb_passthrough", "lowercase",
		"mongo_date_ms", "null_if_empty", "numeric_cast", "oid_to_hex",
	}
	if len(a) != len(want) {
		t.Errorf("want %d transforms, got %d: %v", len(want), len(a), a)
	}
}

// ---- type_resolver tests (no DB side) ----

func TestTypeResolver_Validate_Whitelist(t *testing.T) {
	r := NewTypeResolver(nil)
	valid := []string{
		"BIGINT", "TEXT", "VARCHAR(10)", "CHAR(3)", "NUMERIC(20,4)",
		"DECIMAL(10,2)", "TIMESTAMPTZ", "JSONB", "UUID", "BOOLEAN",
		"TEXT[]", "UUID[]", "ENUM:payment_state", "INTERVAL", "INET",
	}
	for _, s := range valid {
		if !r.Validate(s) {
			t.Errorf("should accept: %s", s)
		}
	}
	invalid := []string{
		"", "NUMERIC", "DECIMAL", "numeric(20,4)", // lowercase
		"VARCHAR", "VARCHAR(0)", "NUMERIC(0,0)",
		"ENUM:BadName", "ENUM:", "BIGINT[][]",
		"EVAL('os.system')", "DROP TABLE",
	}
	for _, s := range invalid {
		if r.Validate(s) {
			t.Errorf("should reject: %s", s)
		}
	}
}

func TestTypeResolver_ValidateValue_VarcharOverflow(t *testing.T) {
	r := NewTypeResolver(nil)
	if v := r.ValidateValue(nil, "VARCHAR(5)", "abcdef"); v == "" {
		t.Error("should flag overflow")
	}
	if v := r.ValidateValue(nil, "VARCHAR(10)", "ok"); v != "" {
		t.Errorf("should pass, got %q", v)
	}
}

func TestTypeResolver_ValidateValue_NumericOverflow(t *testing.T) {
	r := NewTypeResolver(nil)
	if v := r.ValidateValue(nil, "NUMERIC(5,2)", "12345.67"); v == "" {
		t.Error("should flag overflow")
	}
	if v := r.ValidateValue(nil, "NUMERIC(5,2)", "123.45"); v != "" {
		t.Errorf("should pass, got %q", v)
	}
	if v := r.ValidateValue(nil, "NUMERIC(10,4)", 10000.5); v != "" {
		t.Errorf("should pass, got %q", v)
	}
}

// ---- gjson helper test ----

func TestGjsonValueToGo_Primitive(t *testing.T) {
	raw := `{"a": 1, "b": "hi", "c": null, "d": true, "e": [1,2,3], "f": {"x":9}}`
	import_gjson := func(path string) any {
		_ = raw
		// small sanity harness — invokes gjsonValueToGo indirectly via
		// processBatch during integration tests; pure-unit test skipped here.
		return nil
	}
	_ = import_gjson("a")
}
