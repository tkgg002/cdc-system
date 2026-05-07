// Package probes — small, isolated health probes for the CMS system
// health collector. Each probe is a plain function (not a method) so
// the orchestrator (Collector) can call them in any order, in
// parallel, and without dragging the full Collector struct into a
// probe's signature. Status constants are duplicated from the parent
// service package on purpose: probes must NOT import service (would
// be a cycle — service imports probes), and the strings are stable
// wire values consumed by the FE alert banner.
package probes

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"
)

// Section status vocabulary. These string literals mirror
// service.Status* and are part of the JSON contract with the FE.
const (
	StatusOK       = "ok"
	StatusDegraded = "degraded"
	StatusDown     = "down"
	StatusUnknown  = "unknown"
	StatusUp       = "up"
)

// HTTPDeps bundles the shared HTTP plumbing each external probe
// needs. ProbeTimeout caps every individual GET so a slow/dead
// dependency cannot block the orchestrator past its tick budget.
type HTTPDeps struct {
	Client       *http.Client
	ProbeTimeout time.Duration
}

// Get performs a context-bounded GET. The timeout is layered on top
// of the caller's ctx (whichever fires first wins) so the collector's
// errgroup-wide cancel still propagates.
func (d HTTPDeps) Get(ctx context.Context, url string) ([]byte, int, error) {
	ctxQ, cancel := context.WithTimeout(ctx, d.ProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctxQ, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := d.Client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

// SanitizeErr scrubs URLs (and any embedded credentials) out of an
// error string before it lands in the cached snapshot. CLAUDE.md §8
// security gate — the snapshot is served unauthenticated by /healthz
// so it MUST NOT leak internal hostnames or basic-auth tokens.
//
// Scan strategy: walk left-to-right; when "://" is encountered,
// rewind to the start of the scheme prefix and fast-forward past any
// non-whitespace/quote — replace the whole URL slice with a fixed
// marker. Loop continues from the marker's right edge so we don't
// re-scan our own output (which would infinite-loop).
func SanitizeErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	const marker = "<scheme-redacted>"

	var out strings.Builder
	out.Grow(len(msg))
	i := 0
	for i < len(msg) {
		j := strings.Index(msg[i:], "://")
		if j < 0 {
			out.WriteString(msg[i:])
			break
		}
		at := i + j
		start := at
		for start > i && isSchemeByte(msg[start-1]) {
			start--
		}
		end := at + 3
		for end < len(msg) {
			ch := msg[end]
			if ch == ' ' || ch == '"' || ch == '\'' || ch == '\n' || ch == '\t' {
				break
			}
			end++
		}
		out.WriteString(msg[i:start])
		out.WriteString(marker)
		i = end
	}
	return out.String()
}

func isSchemeByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == '+' || b == '-' || b == '.':
		return true
	}
	return false
}
