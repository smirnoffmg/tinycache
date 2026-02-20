package persistence_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"tinycache/internal/persistence"
)

func TestAOF_WriteAndRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "aof.log")

	w, err := persistence.NewAOFWriter(path, "everysec")
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}

	entries := []persistence.AOFEntry{
		{LSN: 1, Op: persistence.OpSet, Key: "foo", Value: []byte("bar"), TTL: 0, Version: 1},
		{LSN: 2, Op: persistence.OpSet, Key: "baz", Value: []byte("qux"), TTL: 100, Version: 2},
		{LSN: 3, Op: persistence.OpDelete, Key: "foo", Value: nil, TTL: 0, Version: 3},
	}

	for i := range entries {
		if err := w.Append(entries[i]); err != nil {
			t.Fatalf("append entry %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reader, err := persistence.NewAOFReader(path)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	var read []persistence.AOFEntry
	for {
		entry, err := reader.Next()
		if err != nil {
			break
		}
		read = append(read, entry)
	}

	if len(read) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(read))
	}

	for i, e := range read {
		if e.LSN != entries[i].LSN {
			t.Errorf("entry %d: LSN got %d, want %d", i, e.LSN, entries[i].LSN)
		}
		if e.Op != entries[i].Op {
			t.Errorf("entry %d: Op got %d, want %d", i, e.Op, entries[i].Op)
		}
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

func TestAOF_CorruptEntry_StopsAtLastValid(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "aof.log")

	w, err := persistence.NewAOFWriter(path, "always")
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}

	for i := range 3 {
		entry := persistence.AOFEntry{
			LSN: uint64(i + 1), Op: persistence.OpSet,
			Key: "key", Value: []byte("val"), Version: uint64(i + 1),
		}
		if err := w.Append(entry); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt the file by truncating the last few bytes
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if err := os.WriteFile(path, data[:len(data)-5], 0o600); err != nil {
		t.Fatalf("write truncated: %v", err)
	}

	reader, err := persistence.NewAOFReader(path)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	count := 0
	for {
		_, err := reader.Next()
		if err != nil {
			break
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 valid entries before corruption, got %d", count)
	}
}

func TestAOF_EmptyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "aof.log")

	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create empty file: %v", err)
	}

	reader, err := persistence.NewAOFReader(path)
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	defer reader.Close()

	_, err = reader.Next()
	if err == nil {
		t.Error("expected error from empty file")
	}
}
