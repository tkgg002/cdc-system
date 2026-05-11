// Package main — per-field profiling logic.
//
// Task -1.1 Section 4. Given a stream of JSONB _raw_data rows, aggregate
// per-field statistics (type distribution, null rate, locale confidence) and
// emit a FieldProfile slice for YAML serialisation.
package main

import (
	"time"

	"github.com/tidwall/gjson"
)

// FieldProfile is the user-facing profile record for ONE JSON key across all
// sampled rows. It is marshalled to YAML as-is.
type FieldProfile struct {
	Field              string             `yaml:"field"`
	DetectedType       string             `yaml:"detected_type"`
	NumberLocale       map[string]float64 `yaml:"number_locale,omitempty"`
	DetectedDateFormat string             `yaml:"detected_date_format,omitempty"`
	Confidence         float64            `yaml:"confidence"`
	NullRate           float64            `yaml:"null_rate"`
	SampleSize         int                `yaml:"sample_size"`
	IsFinancial        bool               `yaml:"is_financial"`
	AdminOverride      string             `yaml:"admin_override,omitempty"`
}

// TableProfile is the top-level YAML document.
type TableProfile struct {
	Table       string         `yaml:"table"`
	SampleSize  int            `yaml:"sample_size"`
	GeneratedAt string         `yaml:"generated_at"`
	Fields      []FieldProfile `yaml:"fields"`
}

// fieldAccumulator holds in-progress stats for one JSON key.
type fieldAccumulator struct {
	seen             int
	nullCount        int
	typeCount        map[string]int  // "number"|"string"|"boolean"|"null"
	stringSamples    []string        // capped
	stringSampleCap  int
}

func newAccumulator() *fieldAccumulator {
	return &fieldAccumulator{
		typeCount:       map[string]int{},
		stringSampleCap: 100,
	}
}

// observe records one observation of a JSON value for this field.
func (f *fieldAccumulator) observe(v gjson.Result) {
	f.seen++
	switch v.Type {
	case gjson.Null:
		f.nullCount++
		f.typeCount["null"]++
	case gjson.False, gjson.True:
		f.typeCount["boolean"]++
	case gjson.Number:
		f.typeCount["number"]++
	case gjson.String:
		f.typeCount["string"]++
		if len(f.stringSamples) < f.stringSampleCap {
			f.stringSamples = append(f.stringSamples, v.String())
		}
	case gjson.JSON:
		// Nested object / array — we don't descend, just classify.
		f.typeCount["string"]++
	default:
		f.typeCount["string"]++
	}
}

// finalize produces the public FieldProfile.
// totalRows is the number of rows sampled overall (used to compute null_rate
// for fields that were simply absent in some rows).
func (f *fieldAccumulator) finalize(name string, totalRows int) FieldProfile {
	fp := FieldProfile{
		Field:       name,
		SampleSize:  f.seen,
		IsFinancial: IsFinancialField(name),
	}
	// null_rate: (absent + null) / totalRows.
	absent := totalRows - f.seen
	if absent < 0 {
		absent = 0
	}
	if totalRows > 0 {
		fp.NullRate = float64(f.nullCount+absent) / float64(totalRows)
	}
	// Detected type: dominant non-null type. If multiple non-null types with
	// comparable counts → "mixed".
	nonNull := map[string]int{}
	nonNullTotal := 0
	for t, c := range f.typeCount {
		if t == "null" {
			continue
		}
		nonNull[t] = c
		nonNullTotal += c
	}
	if nonNullTotal == 0 {
		fp.DetectedType = "null"
		fp.Confidence = 0
	} else {
		bestType := ""
		bestCount := 0
		for t, c := range nonNull {
			if c > bestCount {
				bestType = t
				bestCount = c
			}
		}
		fp.DetectedType = bestType
		fp.Confidence = float64(bestCount) / float64(nonNullTotal)
		// Mixed threshold: if dominant < 0.8 AND we have >=2 types, label mixed.
		if fp.Confidence < 0.8 && len(nonNull) >= 2 {
			fp.DetectedType = "mixed"
		}
	}
	// Locale detection: only meaningful when we actually collected string
	// samples AND the field LOOKS numeric-ish (financial or dominant number).
	if len(f.stringSamples) > 0 && (fp.IsFinancial || nonNull["number"] > 0 || nonNull["string"] > 0) {
		loc := DetectNumberLocale(f.stringSamples)
		if len(loc) > 0 {
			fp.NumberLocale = loc
		}
	}
	// AdminOverride decision:
	//   financial → always REQUIRED.
	//   confidence < 0.95 → REQUIRED.
	//   else → empty (auto-accept).
	if fp.IsFinancial {
		fp.AdminOverride = "REQUIRED"
	} else if fp.Confidence < 0.95 {
		fp.AdminOverride = "REQUIRED"
	}
	return fp
}

// ProfileRows iterates raw JSONB rows and produces a TableProfile.
// rawRows: iterator yielding one _raw_data JSONB byte-slice at a time.
// Returning error from the iterator aborts profiling.
type RowIterator func(yield func(raw []byte) error) error

// ProfileTable drives accumulation using the RowIterator pattern so callers
// can stream from gorm.Rows without materialising all rows in memory.
func ProfileTable(tableName string, iter RowIterator) (*TableProfile, error) {
	accs := map[string]*fieldAccumulator{}
	rows := 0
	err := iter(func(raw []byte) error {
		rows++
		gjson.ParseBytes(raw).ForEach(func(key, val gjson.Result) bool {
			name := key.String()
			acc, ok := accs[name]
			if !ok {
				acc = newAccumulator()
				accs[name] = acc
			}
			acc.observe(val)
			return true
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	tp := &TableProfile{
		Table:       tableName,
		SampleSize:  rows,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Fields:      make([]FieldProfile, 0, len(accs)),
	}
	for name, acc := range accs {
		tp.Fields = append(tp.Fields, acc.finalize(name, rows))
	}
	// Stable-ish ordering: sort by field name for deterministic output.
	sortFieldsByName(tp.Fields)
	return tp, nil
}
