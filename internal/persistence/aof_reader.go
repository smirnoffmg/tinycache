package persistence

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// AOFReader reads AOF entries from a file sequentially.
type AOFReader struct {
	file *os.File
}

// NewAOFReader opens an AOF file for reading.
func NewAOFReader(path string) (*AOFReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &AOFReader{file: f}, nil
}

// Next reads the next AOF entry. Returns io.EOF when no more entries.
// Stops and returns error on corrupt entries (CRC mismatch or short read).
func (r *AOFReader) Next() (AOFEntry, error) {
	header := make([]byte, aofHeaderSize)
	if _, err := io.ReadFull(r.file, header); err != nil {
		return AOFEntry{}, err
	}

	lsn := binary.LittleEndian.Uint64(header[0:8])
	op := OpCode(header[8])
	ttl := int64(binary.LittleEndian.Uint64(header[9:17])) //nolint:gosec // TTL stored as raw bits
	version := binary.LittleEndian.Uint64(header[17:25])
	keyLen := binary.LittleEndian.Uint16(header[25:27])
	valLen := binary.LittleEndian.Uint32(header[27:31])

	payload := make([]byte, int(keyLen)+int(valLen))
	if _, err := io.ReadFull(r.file, payload); err != nil {
		return AOFEntry{}, errors.New("corrupt entry: short payload")
	}

	checksumBuf := make([]byte, aofChecksumSize)
	if _, err := io.ReadFull(r.file, checksumBuf); err != nil {
		return AOFEntry{}, errors.New("corrupt entry: short checksum")
	}

	storedCRC := binary.LittleEndian.Uint32(checksumBuf)

	// Recompute CRC over header + payload
	h := crc32.NewIEEE()
	_, _ = h.Write(header)
	_, _ = h.Write(payload)
	computedCRC := h.Sum32()

	if storedCRC != computedCRC {
		return AOFEntry{}, errors.New("corrupt entry: CRC mismatch")
	}

	key := string(payload[:keyLen])
	var value []byte
	if valLen > 0 {
		value = payload[keyLen:]
	}

	return AOFEntry{
		LSN:     lsn,
		Op:      op,
		TTL:     ttl,
		Version: version,
		Key:     key,
		Value:   value,
	}, nil
}

// Close closes the AOF reader.
func (r *AOFReader) Close() error {
	return r.file.Close()
}
