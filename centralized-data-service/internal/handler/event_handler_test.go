package handler

import "testing"

func TestExtractSourceAndTable_FromSubject(t *testing.T) {
	tests := []struct {
		subject    string
		source     string
		wantDB     string
		wantTable  string
	}{
		{"cdc.goopay.goopay_wallet.wallet_transactions", "", "goopay_wallet", "wallet_transactions"},
		{"cdc.goopay.goopay_payment.payments", "", "goopay_payment", "payments"},
		{"cdc.goopay.goopay_legacy.legacy_refunds", "", "goopay_legacy", "legacy_refunds"},
		// Fallback: parse from source
		{"short.subject", "/debezium/mongodb/goopay_main/users", "goopay_main", "users"},
		// Unknown
		{"x", "y", "unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			db, table := extractSourceAndTable(tt.subject, tt.source)
			if db != tt.wantDB {
				t.Errorf("sourceDB = %q, want %q", db, tt.wantDB)
			}
			if table != tt.wantTable {
				t.Errorf("table = %q, want %q", table, tt.wantTable)
			}
		})
	}
}

func TestExtractPrimaryKey_MongoObjectId(t *testing.T) {
	data := map[string]interface{}{
		"_id": map[string]interface{}{"$oid": "65f1a2b3c4d5e6f7a8b9c0d1"},
	}
	pk := extractPrimaryKey(data, "_id", "mongodb")
	if pk != "65f1a2b3c4d5e6f7a8b9c0d1" {
		t.Errorf("pk = %q, want ObjectId string", pk)
	}
}

func TestExtractPrimaryKey_StringID(t *testing.T) {
	data := map[string]interface{}{"id": "user-123"}
	pk := extractPrimaryKey(data, "id", "mysql")
	if pk != "user-123" {
		t.Errorf("pk = %q, want %q", pk, "user-123")
	}
}

func TestExtractPrimaryKey_NumericID(t *testing.T) {
	data := map[string]interface{}{"id": float64(42)}
	pk := extractPrimaryKey(data, "id", "mysql")
	if pk != "42" {
		t.Errorf("pk = %q, want %q", pk, "42")
	}
}

func TestExtractPrimaryKey_Missing(t *testing.T) {
	data := map[string]interface{}{"name": "test"}
	pk := extractPrimaryKey(data, "id", "mysql")
	if pk != "" {
		t.Errorf("pk = %q, want empty for missing field", pk)
	}
}
