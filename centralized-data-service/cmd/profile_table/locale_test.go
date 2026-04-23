package main

import "testing"

func TestDetectNumberLocale_EnUS(t *testing.T) {
	samples := []string{"1,234.56", "1234.56", "$100.00", "-1,000.00"}
	out := DetectNumberLocale(samples)
	if out["en_US"] < 0.99 {
		t.Fatalf("expected en_US confidence ~1.0, got %v", out)
	}
	if out["vi_VN"] != 0 || out["de_DE"] != 0 {
		t.Fatalf("expected no vi_VN/de_DE, got %v", out)
	}
}

func TestDetectNumberLocale_ViVN(t *testing.T) {
	// Unambiguous vi_VN samples: decimal ',' present, no ambiguous '.NNN'.
	samples := []string{"1.234,56", "1234,56"}
	out := DetectNumberLocale(samples)
	// vi_VN and de_DE tie at 1.0 — they share the format.
	if out["vi_VN"] < 0.99 {
		t.Fatalf("expected vi_VN ~1.0, got %v", out)
	}
	if out["de_DE"] < 0.99 {
		t.Fatalf("expected de_DE ~1.0 (ties vi_VN), got %v", out)
	}
	if out["en_US"] != 0 {
		t.Fatalf("expected no en_US, got %v", out)
	}
}

// TestDetectNumberLocale_ThousandGroupAmbiguous pins the documented behaviour:
// "100.000" / "1.234.567" / "100,000" CANNOT be disambiguated from shape
// alone, so all three locales receive confidence.
func TestDetectNumberLocale_ThousandGroupAmbiguous(t *testing.T) {
	samples := []string{"100.000 đ"}
	out := DetectNumberLocale(samples)
	if out["en_US"] != 1.0 || out["vi_VN"] != 1.0 || out["de_DE"] != 1.0 {
		t.Fatalf("expected all 1.0 for thousand-group shape, got %v", out)
	}
	samples2 := []string{"100,000"}
	out2 := DetectNumberLocale(samples2)
	if out2["en_US"] != 1.0 || out2["vi_VN"] != 1.0 || out2["de_DE"] != 1.0 {
		t.Fatalf("expected all 1.0 for comma thousand-group, got %v", out2)
	}
}

func TestDetectNumberLocale_DeDE(t *testing.T) {
	// Same shape as vi_VN — the test asserts that de_DE is reported
	// with equal confidence (we cannot disambiguate without language tag).
	samples := []string{"1.234,56"}
	out := DetectNumberLocale(samples)
	if out["de_DE"] != out["vi_VN"] {
		t.Fatalf("expected de_DE == vi_VN, got %v", out)
	}
	if out["de_DE"] < 0.99 {
		t.Fatalf("expected de_DE ~1.0, got %v", out)
	}
}

func TestDetectNumberLocale_Ambiguous(t *testing.T) {
	// Digits only → contribute to all three.
	samples := []string{"1234"}
	out := DetectNumberLocale(samples)
	if out["en_US"] != 1.0 || out["vi_VN"] != 1.0 || out["de_DE"] != 1.0 {
		t.Fatalf("expected all locales 1.0 for ambiguous, got %v", out)
	}
}

func TestDetectNumberLocale_Mixed(t *testing.T) {
	// 2 en_US, 2 vi_VN. Total 4. en_US=0.5, vi_VN=0.5, de_DE=0.5.
	samples := []string{"1,234.56", "1234.56", "1.234,56", "1234,56"}
	out := DetectNumberLocale(samples)
	if abs(out["en_US"]-0.5) > 0.001 {
		t.Fatalf("en_US expected 0.5, got %v", out["en_US"])
	}
	if abs(out["vi_VN"]-0.5) > 0.001 {
		t.Fatalf("vi_VN expected 0.5, got %v", out["vi_VN"])
	}
	if abs(out["de_DE"]-0.5) > 0.001 {
		t.Fatalf("de_DE expected 0.5, got %v", out["de_DE"])
	}
}

func TestDetectNumberLocale_Empty(t *testing.T) {
	out := DetectNumberLocale(nil)
	if len(out) != 0 {
		t.Fatalf("expected empty map, got %v", out)
	}
	out2 := DetectNumberLocale([]string{"", "   ", "abc"})
	if len(out2) != 0 {
		t.Fatalf("expected empty map for non-numeric, got %v", out2)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
