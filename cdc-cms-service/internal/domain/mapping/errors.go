// Package mapping — sentinel errors used by command handlers and the
// repository adapters that wrap `cdc_system.mapping_rule_v2`.
//
// Plan v2 §P1 mandated this file as part of the domain skeleton; the
// errors here are the contract the application layer wraps against
// when classifying repo failures into HTTP status codes.
package mapping

import "errors"

// ErrInvalidScope is returned when a Rule references a (source_table,
// target_column) pair that the destination master_binding does not
// declare. Surfaced to the API as 400.
var ErrInvalidScope = errors.New("mapping: invalid scope")

// ErrDuplicate is returned when a Save would violate the
// (source_table, target_column) UNIQUE constraint. Surfaced to the API
// as 409.
var ErrDuplicate = errors.New("mapping: duplicate rule")
