package airbyte

import (
	"strings"
)

// MismatchType defines the type of inconsistency found
type MismatchType string

const (
	MismatchStatus  MismatchType = "status_mismatch"  // e.g. CMS active, Airbyte selected = false
	MismatchMissing MismatchType = "missing_stream"   // e.g. Table in CMS but NOT in Airbyte connection
	MismatchOrphan  MismatchType = "orphan_stream"    // e.g. Stream selected in Airbyte but NOT in CMS Registry
)

// StreamAudit represents the result of comparing a single stream
type StreamAudit struct {
	StreamName    string       `json:"stream_name"`
	TableName     string       `json:"table_name"`
	AirbyteStatus bool         `json:"airbyte_selected"`
	SystemStatus  bool         `json:"system_active"`
	Mismatch      MismatchType `json:"mismatch_type,omitempty"`
}

// CompareCatalog compares the current Airbyte Connection state with the System Registry entries
func CompareCatalog(conn *Connection, registryTables []string, activeTables map[string]bool) []StreamAudit {
	var audits []StreamAudit
	
	// Create a map for quick lookup of registry tables
	registryMap := make(map[string]bool)
	for _, t := range registryTables {
		registryMap[normalizeName(t)] = true
	}

	// 1. Audit streams present in Airbyte
	processedStreams := make(map[string]bool)
	for _, sc := range conn.SyncCatalog.Streams {
		normalizedName := normalizeName(sc.Stream.Name)
		processedStreams[normalizedName] = true

		audit := StreamAudit{
			StreamName:    sc.Stream.Name,
			AirbyteStatus: sc.Config.Selected,
		}

		// Find corresponding system table
		systemActive, inRegistry := activeTables[sc.Stream.Name]
		if !inRegistry {
			// Try normalized lookup
			// (Note: In a real app, we'd store the mapping or ensure names match)
			audit.SystemStatus = false
			if sc.Config.Selected {
				audit.Mismatch = MismatchOrphan
			}
		} else {
			audit.SystemStatus = systemActive
			if audit.AirbyteStatus != audit.SystemStatus {
				audit.Mismatch = MismatchStatus
			}
		}
		
		if audit.Mismatch != "" {
			audits = append(audits, audit)
		}
	}

	// 2. Find tables in Registry that are MISSING in Airbyte Connection
	for _, t := range registryTables {
		if !processedStreams[normalizeName(t)] {
			audits = append(audits, StreamAudit{
				TableName:    t,
				SystemStatus: activeTables[t],
				Mismatch:     MismatchMissing,
			})
		}
	}

	return audits
}

func normalizeName(name string) string {
	return strings.ReplaceAll(strings.ToLower(name), "_", "-")
}
