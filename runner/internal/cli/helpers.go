package cli

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

// tryReadMetadataFromStdin attempts to read metadata from stdin
// Returns (metadata, true) if stdin has data, (nil, false) if no stdin data
func tryReadMetadataFromStdin() ([]map[string]any, bool) {
	// Check if there's data from stdin
	stat, err := os.Stdin.Stat()
	if err != nil {
		log.Printf("❌ Failed to check stdin: %v", err)
		os.Exit(1)
	}

	if (stat.Mode() & os.ModeCharDevice) == 0 {
		// Data is being piped in via stdin
		log.Printf("📥 Reading metadata from stdin...")

		stdinData, err := io.ReadAll(os.Stdin)
		if err != nil {
			log.Printf("❌ Failed to read from stdin: %v", err)
			os.Exit(1)
		}

		// Parse the JSON metadata from stdin
		var metadata []map[string]any
		if err := json.Unmarshal(stdinData, &metadata); err != nil {
			log.Printf("❌ Failed to parse stdin JSON: %v", err)
			os.Exit(1)
		}

		log.Printf("📋 Using %d metadata records from stdin", len(metadata))
		return metadata, true
	}

	return nil, false
}