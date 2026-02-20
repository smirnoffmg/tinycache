package persistence_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"tinycache/internal/persistence"
)

func TestSnapshot_WriteAndRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.rdb")

	entries := []persistence.AOFEntry{
		{LSN: 10, Op: persistence.OpSet, Key: "a", Value: []byte("1"), TTL: 0, Version: 10},
		{LSN: 20, Op: persistence.OpSet, Key: "b", Value: []byte("2"), TTL: 100, Version: 20},
		{LSN: 30, Op: persistence.OpSet, Key: "c", Value: []byte("3"), TTL: 0, Version: 30},
	}

	err := persistence.WriteSnapshot(path, entries)
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	read, maxLSN, err := persistence.ReadSnapshot(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	if maxLSN != 30 {
		t.Errorf("expected max LSN 30, got %d", maxLSN)
	}
	if len(read) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(read))
	}

	for i, e := range read {
		if e.Key != entries[i].Key {
			t.Errorf("entry %d: Key got %q, want %q", i, e.Key, entries[i].Key)
		}
		if !bytes.Equal(e.Value, entries[i].Value) {
			t.Errorf("entry %d: Value got %q, want %q", i, e.Value, entries[i].Value)
		}
		if e.Version != entries[i].Version {
			t.Errorf("entry %d: Version got %d, want %d", i, e.Version, entries[i].Version)
		}
	}
}

func TestSnapshot_CorruptFile_Rejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.rdb")

	entries := []persistence.AOFEntry{
		{LSN: 1, Op: persistence.OpSet, Key: "a", Value: []byte("1"), Version: 1},
	}
	err := persistence.WriteSnapshot(path, entries)
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}

	// Corrupt the CRC at the end
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write corrupted: %v", err)
	}

	_, _, err = persistence.ReadSnapshot(path)
	if err == nil {
		t.Error("expected error for corrupt snapshot")
	}
}

func TestSnapshot_MissingFile(t *testing.T) {
	t.Parallel()

	_, _, err := persistence.ReadSnapshot("/nonexistent/snapshot.rdb")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestSnapshot_AtomicWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.rdb")

	entries := []persistence.AOFEntry{
		{LSN: 1, Op: persistence.OpSet, Key: "a", Value: []byte("1"), Version: 1},
	}
	err := persistence.WriteSnapshot(path, entries)
	if err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	// Verify temp file does not remain
	tmpPath := path + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("temp file should not exist after successful write")
	}
}

func TestRecovery_SnapshotPlusAOF(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.rdb")
	aofPath := filepath.Join(dir, "aof.log")

	// Write a snapshot with LSN up to 10
	snapEntries := []persistence.AOFEntry{
		{LSN: 5, Op: persistence.OpSet, Key: "a", Value: []byte("old-a"), Version: 5},
		{LSN: 10, Op: persistence.OpSet, Key: "b", Value: []byte("b-val"), Version: 10},
	}
	if err := persistence.WriteSnapshot(snapshotPath, snapEntries); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	// Write AOF with entries both before and after the snapshot
	aofWriter, err := persistence.NewAOFWriter(aofPath, "always")
	if err != nil {
		t.Fatalf("create aof writer: %v", err)
	}
	aofEntries := []persistence.AOFEntry{
		{LSN: 8, Op: persistence.OpSet, Key: "a", Value: []byte("should-skip"), Version: 8},
		{LSN: 11, Op: persistence.OpSet, Key: "a", Value: []byte("new-a"), Version: 11},
		{LSN: 12, Op: persistence.OpDelete, Key: "b", Value: nil, Version: 12},
	}
	for i := range aofEntries {
		if err := aofWriter.Append(aofEntries[i]); err != nil {
			t.Fatalf("append aof: %v", err)
		}
	}
	if err := aofWriter.Close(); err != nil {
		t.Fatalf("close aof: %v", err)
	}

	// Recover
	applied := make(map[string]persistence.AOFEntry)
	deleted := make(map[string]bool)

	err = persistence.Recover(snapshotPath, aofPath, func(entry persistence.AOFEntry) {
		if entry.Op == persistence.OpDelete {
			deleted[entry.Key] = true
			delete(applied, entry.Key)
		} else {
			applied[entry.Key] = entry
		}
	})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	// "a" should be "new-a" (from AOF LSN=11, not "old-a" from snapshot or "should-skip" from AOF LSN=8)
	if e, ok := applied["a"]; !ok || string(e.Value) != "new-a" {
		t.Errorf("expected a=new-a, got %v", applied["a"])
	}

	// "b" should be deleted (AOF LSN=12)
	if !deleted["b"] {
		t.Error("expected b to be deleted")
	}
}
