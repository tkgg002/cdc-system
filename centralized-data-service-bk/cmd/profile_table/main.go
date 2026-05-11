// Command profile_table samples a Postgres table's JSONB _raw_data column
// and emits a YAML field profile (per-field detected type, locale confidence,
// financial flag, admin-override decision).
//
// Scope: Task -1.1 — TOOL ONLY. Does NOT run against any shared DB
// automatically; caller must provide --dsn or DB_DSN env var explicitly.
//
// Usage:
//
//	./profile_table --table=payment_bills --sample=5 \
//	    --output=./profiles/payment_bills.profile.yaml \
//	    --dsn="host=... user=... password=... dbname=... sslmode=disable"
//
// Flags:
//
//	--table   required, target table in public schema
//	--sample  BERNOULLI percentage 1..100 (default 5)
//	--output  YAML output path (default: stdout)
//	--dsn     PG DSN (default from env DB_DSN)
package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// tableNameRE restricts --table to safe identifiers. Applied to avoid SQL
// injection because table name cannot be parameterised in Postgres.
var tableNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

func main() {
	var (
		table    = flag.String("table", "", "target table in public schema (required)")
		sample   = flag.Float64("sample", 5, "BERNOULLI sample percentage [1..100]")
		output   = flag.String("output", "", "output YAML path (default stdout)")
		dsn      = flag.String("dsn", os.Getenv("DB_DSN"), "PG DSN (default env DB_DSN)")
	)
	flag.Parse()

	if err := run(*table, *sample, *output, *dsn); err != nil {
		fmt.Fprintf(os.Stderr, "profile_table: %v\n", err)
		os.Exit(1)
	}
}

func run(table string, sample float64, output, dsn string) error {
	if table == "" {
		return errors.New("--table is required")
	}
	if !tableNameRE.MatchString(table) {
		return fmt.Errorf("invalid table name %q: must match %s", table, tableNameRE.String())
	}
	if sample < 1 || sample > 100 {
		return fmt.Errorf("--sample must be in [1..100], got %v", sample)
	}
	if dsn == "" {
		return errors.New("--dsn empty and DB_DSN env not set")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("gorm open: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("db handle: %w", err)
	}
	defer sqlDB.Close()

	profile, err := sampleAndProfile(db, table, sample)
	if err != nil {
		return err
	}
	return writeOutput(profile, output)
}

// sampleAndProfile runs the BERNOULLI query (capped at 5000 rows) and streams
// rows through ProfileTable.
func sampleAndProfile(db *gorm.DB, table string, sample float64) (*TableProfile, error) {
	// Table name already validated; interpolate safely.
	query := fmt.Sprintf(
		`SELECT _raw_data FROM public.%s TABLESAMPLE BERNOULLI($1) LIMIT 5000`,
		table,
	)
	rows, err := db.Raw(query, sample).Rows()
	if err != nil {
		return nil, fmt.Errorf("sample query: %w", err)
	}
	defer rows.Close()

	iter := func(yield func(raw []byte) error) error {
		return streamRows(rows, yield)
	}
	return ProfileTable(table, iter)
}

// streamRows reads each row's _raw_data bytea/jsonb column and forwards to
// yield. Accepts *sql.Rows (from gorm Raw.Rows()).
func streamRows(rows *sql.Rows, yield func(raw []byte) error) error {
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		if len(raw) == 0 {
			continue
		}
		if err := yield(raw); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows err: %w", err)
	}
	return nil
}
