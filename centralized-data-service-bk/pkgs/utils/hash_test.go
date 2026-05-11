package utils

import "testing"

func TestCalculateHash_Deterministic(t *testing.T) {
	data := map[string]interface{}{"id": "123", "amount": 100.5}

	hash1 := CalculateHash(data)
	hash2 := CalculateHash(data)

	if hash1 != hash2 {
		t.Errorf("hash not deterministic: %q != %q", hash1, hash2)
	}
	if len(hash1) != 64 {
		t.Errorf("hash length = %d, want 64 (SHA256 hex)", len(hash1))
	}
}

func TestCalculateHash_DifferentData(t *testing.T) {
	h1 := CalculateHash(map[string]interface{}{"id": "1"})
	h2 := CalculateHash(map[string]interface{}{"id": "2"})

	if h1 == h2 {
		t.Error("different data should produce different hashes")
	}
}
