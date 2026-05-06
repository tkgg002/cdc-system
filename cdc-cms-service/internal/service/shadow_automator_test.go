// shadow_automator_test.go — pure-fn guard for validateIdent.
//
// Project convention: DDL execution paths ship to deploy-time E2E
// (no sqlmock in go.sum). The identifier validator is the only
// non-DB surface here and it's the security gate against SQL
// injection in shadow_<schema>.<table> — pin its behaviour explicitly.
package service

import (
	"strings"
	"testing"
)

func TestValidateIdent_Valid(t *testing.T) {
	ok := []string{
		"orders",
		"shadow_goopay_source",
		"t1",
		"a", // 1 char min
		"users_2026",
		strings.Repeat("a", 63), // 63 char max
	}
	for _, s := range ok {
		if err := validateIdent(s); err != nil {
			t.Errorf("validateIdent(%q): want OK, got %v", s, err)
		}
	}
}

func TestValidateIdent_RejectsInvalid(t *testing.T) {
	bad := []string{
		"",                       // empty
		strings.Repeat("a", 64),  // > 63
		"Orders",                 // uppercase
		"public.orders",          // dot (potential schema injection)
		"orders;DROP",            // semicolon
		"orders ",                // trailing space
		"orders\"",               // quote
		"-leading-dash",          // dash
		"select",                 // keyword would be ok by syntax, but let's confirm — actually all-lowercase letters are accepted
	}
	for _, s := range bad {
		err := validateIdent(s)
		if s == "select" {
			// "select" passes syntax check (lowercase letters only).
			// Caller must rely on quoted-identifier semantics — pin
			// that this validator does NOT block keywords.
			if err != nil {
				t.Errorf("validateIdent(%q): keyword should pass syntax (caller quotes), got %v", s, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("validateIdent(%q): want error, got nil", s)
		}
	}
}
