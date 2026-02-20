package persistence

import (
	"errors"
	"log"
	"os"
)

// Recover loads state from snapshot + AOF.
// The apply callback is called for each entry that should be applied to the store.
// Entries from AOF with LSN <= snapshot's max LSN are skipped.
func Recover(snapshotPath, aofPath string, apply func(entry AOFEntry)) error {
	var snapshotLSN uint64

	if _, err := os.Stat(snapshotPath); err == nil {
		entries, maxLSN, err := ReadSnapshot(snapshotPath)
		if err != nil {
			log.Printf("snapshot corrupt, skipping: %v", err)
		} else {
			snapshotLSN = maxLSN
			for _, e := range entries {
				apply(e)
			}
		}
	}

	if _, err := os.Stat(aofPath); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	reader, err := NewAOFReader(aofPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	for {
		entry, err := reader.Next()
		if err != nil {
			break
		}
		if entry.LSN <= snapshotLSN {
			continue
		}
		apply(entry)
	}

	return nil
}
