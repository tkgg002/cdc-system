// Package utils — pg_ident.go
//
// PgIdent quotes a Postgres identifier safely. Only alphanumerics +
// underscore are allowed; anything else returns an empty-quoted
// identifier (`""`) which the Postgres parser rejects deliberately —
// fail-closed so a malformed table name cannot become a SQL injection
// surface in callers that interpolate via fmt.Sprintf.
//
// Plan v2 §P4 / T4.8 lifted this helper from
// `internal/api/reconciliation_handler.go` so identifier quoting lives
// at the boundary (utils) rather than in the API layer.
package utils

// PgIdent returns the input wrapped in double quotes when it contains
// only [a-zA-Z0-9_] characters. Otherwise returns `""` so the SQL
// statement fails parsing rather than silently executing with a
// potentially injected identifier.
func PgIdent(name string) string {
	if name == "" {
		return `""`
	}
	for _, r := range name {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return `""`
		}
	}
	return `"` + name + `"`
}
