// Package main — YAML output.
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"gopkg.in/yaml.v3"
)

// sortFieldsByName sorts FieldProfile slice alphabetically.
func sortFieldsByName(fs []FieldProfile) {
	sort.SliceStable(fs, func(i, j int) bool {
		return fs[i].Field < fs[j].Field
	})
}

// marshalYAML produces pretty-indented YAML bytes.
func marshalYAML(tp *TableProfile) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(tp); err != nil {
		return nil, fmt.Errorf("yaml encode: %w", err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("yaml close: %w", err)
	}
	return buf.Bytes(), nil
}

// writeOutput writes YAML to outputPath or stdout if outputPath == "".
// Creates parent directories if needed.
func writeOutput(tp *TableProfile, outputPath string) error {
	data, err := marshalYAML(tp)
	if err != nil {
		return err
	}
	if outputPath == "" {
		_, err := io.Copy(os.Stdout, bytes.NewReader(data))
		return err
	}
	if dir := filepath.Dir(outputPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(outputPath, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", outputPath, err)
	}
	return nil
}
