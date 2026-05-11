// Package migrations bundles every .sql at build-time via go:embed so the
// binary applies them on startup without depending on the current working
// directory or a side-channel `make migrate` step.
package migrations

import "embed"

// Files holds every numbered migration shipped with this binary. Use
// fs.ReadDir(Files, ".") to enumerate in lexicographic order; filenames
// follow `NNN_description.sql` so lexicographic == numeric.
//
//go:embed *.sql
var Files embed.FS
