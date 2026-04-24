package service

import "testing"

func TestSchemaInspectorMasksTopLevelSampleValue(t *testing.T) {
	inspector := &SchemaInspector{}
	inspector.SetMaskingService(NewMaskingService(nil, nil, "phone", "email"))

	if got := inspector.maskSampleValue("customer_profiles", "phone", "0901234567"); got != "***" {
		t.Fatalf("phone should be masked, got %#v", got)
	}
	if got := inspector.maskSampleValue("customer_profiles", "email", "alice@example.com"); got != "***" {
		t.Fatalf("email should be masked, got %#v", got)
	}
	if got := inspector.maskSampleValue("customer_profiles", "name", "Alice"); got != "Alice" {
		t.Fatalf("non-sensitive field changed unexpectedly, got %#v", got)
	}
}

func TestSchemaInspectorMasksNestedSampleValue(t *testing.T) {
	inspector := &SchemaInspector{}
	inspector.SetMaskingService(NewMaskingService(nil, nil, "balance"))

	got := inspector.maskSampleValue("wallet_accounts", "metadata", map[string]interface{}{
		"user_info": map[string]interface{}{
			"balance": float64(125000),
			"tier":    "gold",
		},
	})

	metadata := got.(map[string]interface{})
	userInfo := metadata["user_info"].(map[string]interface{})
	if userInfo["balance"] != "***" {
		t.Fatalf("nested balance should be masked, got %#v", userInfo["balance"])
	}
	if userInfo["tier"] != "gold" {
		t.Fatalf("non-sensitive nested field changed unexpectedly, got %#v", userInfo["tier"])
	}
}

func TestSchemaInspectorMasksArraySampleValue(t *testing.T) {
	inspector := &SchemaInspector{}
	inspector.SetMaskingService(NewMaskingService(nil, nil, "phone"))

	got := inspector.maskSampleValue("crm_contacts", "contacts", []interface{}{
		map[string]interface{}{"phone_number": "0901", "label": "home"},
	})

	contacts := got.([]interface{})
	first := contacts[0].(map[string]interface{})
	if first["phone_number"] != "***" {
		t.Fatalf("array phone_number should be masked, got %#v", first["phone_number"])
	}
	if first["label"] != "home" {
		t.Fatalf("non-sensitive array field changed unexpectedly, got %#v", first["label"])
	}
}

func TestSchemaInspectorHeuristicMaskingWithoutRegistry(t *testing.T) {
	inspector := &SchemaInspector{}

	for _, tc := range []struct {
		field string
		value interface{}
	}{
		{field: "customer_secret", value: "token-123"},
		{field: "remaining_balance", value: 5000},
		{field: "altPhone", value: "0902"},
	} {
		if got := inspector.maskSampleValue("orders", tc.field, tc.value); got != "***" {
			t.Fatalf("%s should be masked heuristically, got %#v", tc.field, got)
		}
	}

	if got := inspector.maskSampleValue("orders", "status", "active"); got != "active" {
		t.Fatalf("heuristic masking touched non-sensitive field, got %#v", got)
	}
}
