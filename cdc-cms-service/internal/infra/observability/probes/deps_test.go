// deps_test.go — pure-fn coverage for SanitizeErr.
//
// SanitizeErr is the security gate per CLAUDE.md §8: the snapshot
// is served unauthenticated by /healthz so error strings MUST NOT
// leak internal hostnames or basic-auth tokens. Any drift here is
// a security regression — pin it explicitly.
package probes

import (
	"errors"
	"strings"
	"testing"
)

func TestSanitizeErr_NilIsEmpty(t *testing.T) {
	if got := SanitizeErr(nil); got != "" {
		t.Errorf("nil err: want empty, got %q", got)
	}
}

func TestSanitizeErr_NoURLIsVerbatim(t *testing.T) {
	in := "connection refused after 3 retries"
	if got := SanitizeErr(errors.New(in)); got != in {
		t.Errorf("no URL: want verbatim, got %q", got)
	}
}

func TestSanitizeErr_RedactsSingleURL(t *testing.T) {
	err := errors.New(`Get "http://internal-host:8083/connectors": dial tcp: i/o timeout`)
	got := SanitizeErr(err)
	if strings.Contains(got, "internal-host") || strings.Contains(got, "8083") {
		t.Errorf("internal host leaked: %q", got)
	}
	if !strings.Contains(got, "<scheme-redacted>") {
		t.Errorf("missing redaction marker: %q", got)
	}
	if !strings.Contains(got, "i/o timeout") {
		t.Errorf("non-URL context dropped: %q", got)
	}
}

func TestSanitizeErr_RedactsAuthTokens(t *testing.T) {
	// basic-auth credentials inline in URL must not survive.
	err := errors.New(`auth failed at https://admin:s3cr3t@vault.internal/v1/login`)
	got := SanitizeErr(err)
	if strings.Contains(got, "s3cr3t") || strings.Contains(got, "vault.internal") {
		t.Fatalf("credential or host leaked: %q", got)
	}
}

func TestSanitizeErr_RedactsMultipleURLs(t *testing.T) {
	err := errors.New(`fanout: http://a:1/x failed; http://b:2/y also failed`)
	got := SanitizeErr(err)
	if strings.Contains(got, "http://") {
		t.Errorf("URL not fully redacted: %q", got)
	}
	// at least 2 redaction markers expected.
	if c := strings.Count(got, "<scheme-redacted>"); c < 2 {
		t.Errorf("want ≥2 markers, got %d in %q", c, got)
	}
}

func TestSanitizeErr_PreservesQuotedTail(t *testing.T) {
	// URL inside double quotes — redaction stops at the closing quote.
	err := errors.New(`Get "http://h:1/x" failed`)
	got := SanitizeErr(err)
	if strings.Contains(got, "h:1") {
		t.Errorf("URL leaked: %q", got)
	}
	if !strings.Contains(got, "failed") {
		t.Errorf("trailing context dropped: %q", got)
	}
}

func TestIsSchemeByte(t *testing.T) {
	want := map[byte]bool{
		'a': true, 'z': true, 'A': true, 'Z': true,
		'0': true, '9': true,
		'+': true, '-': true, '.': true,
		' ': false, '/': false, ':': false, '@': false, '_': false,
	}
	for b, w := range want {
		if got := isSchemeByte(b); got != w {
			t.Errorf("isSchemeByte(%q): got %v want %v", b, got, w)
		}
	}
}
