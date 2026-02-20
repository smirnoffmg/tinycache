package persistence

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"time"
)

const (
	snapshotMagic   = "TCDB"
	snapshotVersion = uint16(1)
)

// WriteSnapshot atomically writes a full snapshot to the given path.
// Uses write-to-temp, fsync, rename pattern for crash safety.
func WriteSnapshot(path string, entries []AOFEntry) error {
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	h := crc32.NewIEEE()

	// Header: magic(4) + version(2) + count(8) + timestamp(8) = 22 bytes
	header := make([]byte, 22)
	copy(header[0:4], snapshotMagic)
	binary.LittleEndian.PutUint16(header[4:6], snapshotVersion)
	binary.LittleEndian.PutUint64(header[6:14], uint64(len(entries)))
	binary.LittleEndian.PutUint64(header[14:22], uint64(time.Now().UnixNano()))

	if _, err := f.Write(header); err != nil {
		_ = f.Close()
		return err
	}
	_, _ = h.Write(header)

	for i := range entries {
		data := marshalAOFEntry(entries[i])
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return err
		}
		_, _ = h.Write(data)
	}

	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, h.Sum32())
	if _, err := f.Write(crcBuf); err != nil {
		_ = f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

// ReadSnapshot reads a snapshot file and returns all entries and the max LSN.
func ReadSnapshot(path string) ([]AOFEntry, uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	if len(data) < 26 { // 22 header + 4 CRC minimum
		return nil, 0, errors.New("snapshot too small")
	}

	if string(data[0:4]) != snapshotMagic {
		return nil, 0, errors.New("invalid snapshot magic")
	}

	entryCount := binary.LittleEndian.Uint64(data[6:14])

	bodyData := data[:len(data)-4]
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	computedCRC := crc32.ChecksumIEEE(bodyData)
	if storedCRC != computedCRC {
		return nil, 0, errors.New("snapshot CRC mismatch")
	}

	// Parse entries from offset 22
	entries := make([]AOFEntry, 0, entryCount)
	offset := 22
	var maxLSN uint64

	for range entryCount {
		if offset+aofHeaderSize > len(bodyData) {
			return nil, 0, errors.New("snapshot truncated")
		}

		keyLen := binary.LittleEndian.Uint16(bodyData[offset+25 : offset+27])
		valLen := binary.LittleEndian.Uint32(bodyData[offset+27 : offset+31])
		entrySize := aofHeaderSize + int(keyLen) + int(valLen) + aofChecksumSize

		if offset+entrySize > len(bodyData) {
			return nil, 0, errors.New("snapshot entry truncated")
		}

		lsn := binary.LittleEndian.Uint64(bodyData[offset : offset+8])
		op := OpCode(bodyData[offset+8])
		rawTTL := binary.LittleEndian.Uint64(bodyData[offset+9 : offset+17])
		ttl := int64(rawTTL) //nolint:gosec // TTL stored as raw bits
		version := binary.LittleEndian.Uint64(bodyData[offset+17 : offset+25])
		key := string(bodyData[offset+31 : offset+31+int(keyLen)])

		var value []byte
		if valLen > 0 {
			value = make([]byte, valLen)
			copy(value, bodyData[offset+31+int(keyLen):offset+31+int(keyLen)+int(valLen)])
		}

		entries = append(entries, AOFEntry{
			LSN:     lsn,
			Op:      op,
			TTL:     ttl,
			Version: version,
			Key:     key,
			Value:   value,
		})

		if lsn > maxLSN {
			maxLSN = lsn
		}
		offset += entrySize
	}

	return entries, maxLSN, nil
}
