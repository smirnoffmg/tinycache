package cache_test

import (
	"testing"
	"tinycache/internal/cache"
	"tinycache/internal/persistence"
)

type recordingAppender struct {
	entries []persistence.AOFEntry
}

func (r *recordingAppender) Append(entry persistence.AOFEntry) error {
	r.entries = append(r.entries, entry)
	return nil
}

func TestPersistentStore_SetAppendsAOF(t *testing.T) {
	t.Parallel()
	inner := cache.NewStore(256, 0)
	rec := &recordingAppender{}
	lsn := &persistence.LSNGenerator{}
	ps := cache.NewPersistentStore(inner, rec, lsn)

	ps.Set("k", []byte("v"), 0, 300)

	if len(rec.entries) != 1 {
		t.Fatalf("expected 1 AOF entry, got %d", len(rec.entries))
	}
	e := rec.entries[0]
	if e.Op != persistence.OpSet {
		t.Errorf("expected OpSet, got %d", e.Op)
	}
	if e.Key != "k" {
		t.Errorf("expected key 'k', got %q", e.Key)
	}
	if string(e.Value) != "v" {
		t.Errorf("expected value 'v', got %q", e.Value)
	}
	if e.LSN != 1 {
		t.Errorf("expected LSN 1, got %d", e.LSN)
	}
}

func TestPersistentStore_DeleteAppendsAOF(t *testing.T) {
	t.Parallel()
	inner := cache.NewStore(256, 0)
	rec := &recordingAppender{}
	lsn := &persistence.LSNGenerator{}
	ps := cache.NewPersistentStore(inner, rec, lsn)

	ps.Set("k", []byte("v"), 0, 0)
	ps.Delete("k")

	if len(rec.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(rec.entries))
	}
	if rec.entries[1].Op != persistence.OpDelete {
		t.Errorf("expected OpDelete, got %d", rec.entries[1].Op)
	}
}

func TestPersistentStore_FlushAllAppendsAOF(t *testing.T) {
	t.Parallel()
	inner := cache.NewStore(256, 0)
	rec := &recordingAppender{}
	lsn := &persistence.LSNGenerator{}
	ps := cache.NewPersistentStore(inner, rec, lsn)

	ps.Set("a", []byte("1"), 0, 0)
	ps.FlushAll()

	if len(rec.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(rec.entries))
	}
	if rec.entries[1].Op != persistence.OpFlushAll {
		t.Errorf("expected OpFlushAll, got %d", rec.entries[1].Op)
	}
}

func TestPersistentStore_AddAppendsOnlyOnSuccess(t *testing.T) {
	t.Parallel()
	inner := cache.NewStore(256, 0)
	rec := &recordingAppender{}
	lsn := &persistence.LSNGenerator{}
	ps := cache.NewPersistentStore(inner, rec, lsn)

	ps.Set("k", []byte("v"), 0, 0)
	countBefore := len(rec.entries)

	ps.Add("k", []byte("v2"), 0, 0) // should fail, key exists
	if len(rec.entries) != countBefore {
		t.Error("Add on existing key should not append to AOF")
	}

	ps.Add("new", []byte("v"), 0, 0)
	if len(rec.entries) != countBefore+1 {
		t.Error("Add on new key should append to AOF")
	}
}

func TestPersistentStore_ReplaceAppendsOnlyOnSuccess(t *testing.T) {
	t.Parallel()
	inner := cache.NewStore(256, 0)
	rec := &recordingAppender{}
	lsn := &persistence.LSNGenerator{}
	ps := cache.NewPersistentStore(inner, rec, lsn)

	ps.Replace("missing", []byte("v"), 0, 0) // should fail
	if len(rec.entries) != 0 {
		t.Error("Replace on missing key should not append to AOF")
	}

	ps.Set("k", []byte("v"), 0, 0)
	countBefore := len(rec.entries)
	ps.Replace("k", []byte("v2"), 0, 0)
	if len(rec.entries) != countBefore+1 {
		t.Error("Replace on existing key should append to AOF")
	}
}

func TestPersistentStore_CasAppendsOnlyOnStored(t *testing.T) {
	t.Parallel()
	inner := cache.NewStore(256, 0)
	rec := &recordingAppender{}
	lsn := &persistence.LSNGenerator{}
	ps := cache.NewPersistentStore(inner, rec, lsn)

	ps.Set("k", []byte("v"), 0, 0)
	e, _ := ps.Gets("k")
	countBefore := len(rec.entries)

	ps.Cas("k", []byte("new"), 0, 0, 99999) // wrong token
	if len(rec.entries) != countBefore {
		t.Error("CAS with wrong token should not append to AOF")
	}

	ps.Cas("k", []byte("new"), 0, 0, e.CasToken) // correct token
	if len(rec.entries) != countBefore+1 {
		t.Error("CAS with correct token should append to AOF")
	}
}

func TestPersistentStore_DelegatesReads(t *testing.T) {
	t.Parallel()
	inner := cache.NewStore(256, 0)
	rec := &recordingAppender{}
	lsn := &persistence.LSNGenerator{}
	ps := cache.NewPersistentStore(inner, rec, lsn)

	ps.Set("k", []byte("v"), 42, 0)

	e, ok := ps.Get("k")
	if !ok {
		t.Fatal("expected key to exist")
	}
	assertEqual(t, string(e.Value), "v")
	assertEqual(t, e.Flags, uint32(42))

	stats := ps.Stats()
	if stats.Items != 1 {
		t.Errorf("expected 1 item, got %d", stats.Items)
	}
}
