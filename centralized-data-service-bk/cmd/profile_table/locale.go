// Package main — number locale detection.
//
// Task -1.1 Section 4 Data Profiling. Analyze stringified numeric samples
// to decide whether values use:
//   - en_US:  thousand ',' decimal '.'  (e.g. "1,234.56", "1234.56")
//   - vi_VN:  thousand '.' decimal ','  (e.g. "1.234,56", "1234,56")
//   - de_DE:  same format as vi_VN. On tie we contribute to both.
//
// Output is a confidence map (count / total) per locale, lossless so the
// admin reviewer can see ambiguity explicitly.
package main

import "strings"

// DetectNumberLocale returns a map[locale] -> confidence (0..1) from a slice
// of raw string samples. Empty input → empty map.
//
// Decision tree (per sample, after currency/whitespace strip):
//  1. No ',' AND no '.' → "digits only": contribute to en_US + vi_VN + de_DE.
//  2. Only '.' AND every '.' is followed by exactly 3 digits (e.g. "100.000",
//     "1.234.567") → AMBIGUOUS thousand-group: could be en_US integer with
//     dot-thousand confusion, but far more commonly vi_VN/de_DE thousands.
//     Contribute to en_US + vi_VN + de_DE (explicit ambiguity).
//  3. Only '.' otherwise (e.g. "1234.56", "1.5") → en_US (decimal dot).
//  4. Only ',' AND every ',' is followed by exactly 3 digits (e.g. "100,000",
//     "1,234,567") → AMBIGUOUS thousand-group (en_US thousands or vi_VN
//     integer-with-comma). Contribute to all three.
//  5. Only ',' otherwise (e.g. "1234,56", "1,5") → vi_VN + de_DE.
//  6. Both ',' and '.':
//     - last '.' index > last ',' index → en_US (thousands ',', decimal '.').
//     - last ',' index > last '.' index → vi_VN + de_DE.
//  7. Anything with no digits → skipped.
//
// Confidence = count / scored_total. scored_total counts samples that
// contributed to ANY bucket (ambiguous samples count once toward total, even
// though they increment all three buckets; this is intentional — an all-
// ambiguous dataset returns 1.0 confidence per locale, making the tie
// explicit rather than masked).
func DetectNumberLocale(samples []string) map[string]float64 {
	out := map[string]float64{}
	if len(samples) == 0 {
		return out
	}
	var enUS, viVN, deDE, total int
	for _, raw := range samples {
		s := cleanNumericSample(raw)
		if s == "" {
			continue
		}
		hasComma := strings.Contains(s, ",")
		hasDot := strings.Contains(s, ".")
		if !hasComma && !hasDot {
			// All digits / sign only — ambiguous, contribute to all.
			if !hasAnyDigit(s) {
				continue
			}
			enUS++
			viVN++
			deDE++
			total++
			continue
		}
		if !hasAnyDigit(s) {
			// Pure punctuation, skip.
			continue
		}
		if hasDot && !hasComma {
			// Thousand-group ambiguity: "100.000", "1.234.567"
			if allSeparatorsFollowedBy3Digits(s, '.') {
				enUS++
				viVN++
				deDE++
			} else {
				enUS++
			}
			total++
			continue
		}
		if hasComma && !hasDot {
			if allSeparatorsFollowedBy3Digits(s, ',') {
				enUS++
				viVN++
				deDE++
			} else {
				viVN++
				deDE++
			}
			total++
			continue
		}
		// Both present.
		lastDot := strings.LastIndex(s, ".")
		lastComma := strings.LastIndex(s, ",")
		if lastDot > lastComma {
			enUS++
		} else {
			viVN++
			deDE++
		}
		total++
	}
	if total == 0 {
		return out
	}
	denom := float64(total)
	if enUS > 0 {
		out["en_US"] = float64(enUS) / denom
	}
	if viVN > 0 {
		out["vi_VN"] = float64(viVN) / denom
	}
	if deDE > 0 {
		out["de_DE"] = float64(deDE) / denom
	}
	return out
}

// cleanNumericSample strips whitespace, currency markers and a single leading
// minus sign. Returns the inner numeric candidate (may still contain ',' and
// '.'). Empty string means "not a numeric candidate".
func cleanNumericSample(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	// Strip common currency prefixes/suffixes.
	prefixes := []string{"$", "₫", "€", "£", "¥", "USD", "VND", "EUR"}
	for _, p := range prefixes {
		s = strings.TrimPrefix(strings.TrimSpace(s), p)
		s = strings.TrimSpace(s)
	}
	suffixes := []string{"đ", "VND", "USD", "EUR"}
	for _, sfx := range suffixes {
		s = strings.TrimSuffix(strings.TrimSpace(s), sfx)
		s = strings.TrimSpace(s)
	}
	// Single leading minus.
	s = strings.TrimPrefix(s, "-")
	s = strings.TrimSpace(s)
	return s
}

// hasAnyDigit reports whether s contains an ASCII digit 0-9.
func hasAnyDigit(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			return true
		}
	}
	return false
}

// allSeparatorsFollowedBy3Digits reports whether every occurrence of sep in s
// is followed by exactly 3 ASCII digits before either end-of-string or the
// next separator — the hallmark of a "thousand group" formatting where the
// integer portion has no fractional tail. Used to mark ambiguous values like
// "100.000" or "1,234,567" where we cannot disambiguate locale from shape
// alone.
func allSeparatorsFollowedBy3Digits(s string, sep byte) bool {
	seen := false
	for i := 0; i < len(s); i++ {
		if s[i] != sep {
			continue
		}
		seen = true
		// Count digits immediately after i.
		j := i + 1
		digits := 0
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			digits++
			j++
		}
		if digits != 3 {
			return false
		}
		// Next non-digit (if any) must be the same separator or EOS.
		if j < len(s) && s[j] != sep {
			return false
		}
	}
	return seen
}
