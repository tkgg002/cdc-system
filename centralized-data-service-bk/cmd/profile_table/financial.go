// Package main — financial field detection.
//
// Task -1.1 (sonyflake v1.2.5 v7.1 final, Section 4 Data Profiling).
// Goal: identify JSONB field names that refer to monetary / financial values
// so the profiler can flag them as "admin_override = REQUIRED" regardless of
// numeric confidence. Regex is intentionally conservative (case-insensitive)
// to minimize false negatives on real banking payload shapes.
package main

import "regexp"

// financialPatterns lists ALL regular expressions considered "financial".
// Any match → IsFinancialField returns true → AdminOverride=REQUIRED.
//
// Patterns (case-insensitive):
//  1. Exact prefixes of common monetary tokens (amount, balance, currency,
//     account, price, fee, total, sum, refund, payment, transaction) with
//     optional snake_case/numeric suffix.
//  2. Suffix form: *_amount / *_balance / *_price / *_fee / *_total / *_sum.
//  3. Banking verbs: debit / credit / charge / deposit / withdraw / transfer /
//     settlement with optional suffix.
var financialPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^(amount|balance|currency|account|price|fee|total|sum|refund|payment|transaction)([_a-z0-9]*)$`),
	regexp.MustCompile(`(?i)^.+_(amount|balance|price|fee|total|sum)$`),
	regexp.MustCompile(`(?i)^(debit|credit|charge|deposit|withdraw|transfer|settlement)([_a-z0-9]*)$`),
}

// IsFinancialField reports whether a JSONB field name looks financial.
// Matching rule: return true if ANY pattern in financialPatterns matches.
func IsFinancialField(name string) bool {
	for _, re := range financialPatterns {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}
