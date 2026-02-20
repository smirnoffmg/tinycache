package persistence

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

type OpCode byte

const (
	OpSet      OpCode = 0x01
	OpDelete   OpCode = 0x02
	OpFlushAll OpCode = 0x03
)

// AOFEntry represents one record in the AOF.
// Binary format: LSN(8) + Op(1) + TTL(8) + Version(8) + key_len(2) + val_len(4) + key + value + CRC32(4)
type AOFEntry struct {
	LSN     uint64
	Op      OpCode
	TTL     int64
	Version uint64
	Key     string
	Value   []byte
	Flags   uint32
}

const (
	aofHeaderSize   = 8 + 1 + 8 + 8 + 2 + 4 // 31 bytes fixed header
	aofChecksumSize = 4
)

// AOFWriter writes AOF entries to a file.
type AOFWriter struct {
	file     *os.File
	mu       sync.Mutex
	fsyncStr string
	stopOnce sync.Once
	done     chan struct{}
}

// NewAOFWriter creates a new AOF writer. fsyncMode is "always", "everysec", or "no".
func NewAOFWriter(path, fsyncMode string) (*AOFWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}

	w := &AOFWriter{
		file:     f,
		fsyncStr: fsyncMode,
		done:     make(chan struct{}),
	}

	if fsyncMode == "everysec" {
		go w.periodicFsync()
	}
	return w, nil
}

// Append writes one AOF entry to the file.
func (w *AOFWriter) Append(entry AOFEntry) error {
	data := marshalAOFEntry(entry)

	w.mu.Lock()
	_, err := w.file.Write(data)
	if err != nil {
		w.mu.Unlock()
		return err
	}

	if w.fsyncStr == "always" {
		err = w.file.Sync()
	}
	w.mu.Unlock()
	return err
}

// Close flushes and closes the AOF file.
func (w *AOFWriter) Close() error {
	w.stopOnce.Do(func() { close(w.done) })
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.file.Sync()
	return w.file.Close()
}

func (w *AOFWriter) periodicFsync() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.mu.Lock()
			_ = w.file.Sync()
			w.mu.Unlock()
		}
	}
}

func marshalAOFEntry(e AOFEntry) []byte {
	keyBytes := []byte(e.Key)
	totalSize := aofHeaderSize + len(keyBytes) + len(e.Value) + aofChecksumSize
	buf := make([]byte, totalSize)

	binary.LittleEndian.PutUint64(buf[0:8], e.LSN)
	buf[8] = byte(e.Op)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(e.TTL)) //nolint:gosec // TTL stored as raw bits
	binary.LittleEndian.PutUint64(buf[17:25], e.Version)
	kl := uint16(len(keyBytes)) //nolint:gosec // key max 250 bytes per memcached spec
	vl := uint32(len(e.Value))  //nolint:gosec // value bounded by max_memory
	binary.LittleEndian.PutUint16(buf[25:27], kl)
	binary.LittleEndian.PutUint32(buf[27:31], vl)

	copy(buf[31:], keyBytes)
	copy(buf[31+len(keyBytes):], e.Value)

	checksum := crc32.ChecksumIEEE(buf[:totalSize-aofChecksumSize])
	binary.LittleEndian.PutUint32(buf[totalSize-aofChecksumSize:], checksum)

	return buf
}
