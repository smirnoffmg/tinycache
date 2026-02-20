package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type CommandType int

const (
	CmdGet CommandType = iota
	CmdGets
	CmdSet
	CmdAdd
	CmdReplace
	CmdCas
	CmdDelete
	CmdFlushAll
	CmdStats
	CmdQuit
)

type Command struct {
	Type      CommandType
	Keys      []string
	Flags     uint32
	Exptime   int64
	Bytes     int
	CasUnique uint64
	Data      []byte
	Noreply   bool
}

var errUnknownCommand = errors.New("unknown command")

// Parse reads one complete Memcached text protocol command from the reader.
func Parse(r *bufio.Reader) (Command, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return Command{}, fmt.Errorf("read command line: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return Command{}, errors.New("empty command")
	}

	parts := strings.Fields(line)
	if len(parts) == 0 {
		return Command{}, errors.New("empty command")
	}

	switch parts[0] {
	case "get":
		return parseRetrieval(CmdGet, parts)
	case "gets":
		return parseRetrieval(CmdGets, parts)
	case "set":
		return parseStorage(CmdSet, parts, r)
	case "add":
		return parseStorage(CmdAdd, parts, r)
	case "replace":
		return parseStorage(CmdReplace, parts, r)
	case "cas":
		return parseCas(parts, r)
	case "delete":
		return parseDelete(parts)
	case "flush_all":
		return parseFlushAll(parts)
	case "stats":
		return Command{Type: CmdStats}, nil
	case "quit":
		return Command{Type: CmdQuit}, nil
	default:
		return Command{}, fmt.Errorf("%w: %s", errUnknownCommand, parts[0])
	}
}

func parseRetrieval(ct CommandType, parts []string) (Command, error) {
	if len(parts) < 2 {
		return Command{}, fmt.Errorf("%s requires at least one key", parts[0])
	}
	return Command{
		Type: ct,
		Keys: parts[1:],
	}, nil
}

// parseStorage handles: <cmd> <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
func parseStorage(ct CommandType, parts []string, r *bufio.Reader) (Command, error) {
	if len(parts) < 5 {
		return Command{}, fmt.Errorf("%s requires key, flags, exptime, bytes", parts[0])
	}

	flags, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return Command{}, fmt.Errorf("invalid flags: %w", err)
	}

	exptime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("invalid exptime: %w", err)
	}

	byteCount, err := strconv.Atoi(parts[4])
	if err != nil {
		return Command{}, fmt.Errorf("invalid byte count: %w", err)
	}

	noreply := len(parts) > 5 && parts[5] == "noreply"

	data, err := readData(r, byteCount)
	if err != nil {
		return Command{}, err
	}

	return Command{
		Type:    ct,
		Keys:    []string{parts[1]},
		Flags:   uint32(flags),
		Exptime: exptime,
		Bytes:   byteCount,
		Data:    data,
		Noreply: noreply,
	}, nil
}

// parseCas handles: cas <key> <flags> <exptime> <bytes> <cas_unique> [noreply]\r\n<data>\r\n
func parseCas(parts []string, r *bufio.Reader) (Command, error) {
	if len(parts) < 6 {
		return Command{}, errors.New("cas requires key, flags, exptime, bytes, cas_unique")
	}

	flags, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return Command{}, fmt.Errorf("invalid flags: %w", err)
	}

	exptime, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("invalid exptime: %w", err)
	}

	byteCount, err := strconv.Atoi(parts[4])
	if err != nil {
		return Command{}, fmt.Errorf("invalid byte count: %w", err)
	}

	casUnique, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("invalid cas unique: %w", err)
	}

	noreply := len(parts) > 6 && parts[6] == "noreply"

	data, err := readData(r, byteCount)
	if err != nil {
		return Command{}, err
	}

	return Command{
		Type:      CmdCas,
		Keys:      []string{parts[1]},
		Flags:     uint32(flags),
		Exptime:   exptime,
		Bytes:     byteCount,
		CasUnique: casUnique,
		Data:      data,
		Noreply:   noreply,
	}, nil
}

func parseDelete(parts []string) (Command, error) {
	if len(parts) < 2 {
		return Command{}, errors.New("delete requires a key")
	}
	noreply := len(parts) > 2 && parts[2] == "noreply"
	return Command{
		Type:    CmdDelete,
		Keys:    []string{parts[1]},
		Noreply: noreply,
	}, nil
}

func parseFlushAll(parts []string) (Command, error) {
	cmd := Command{Type: CmdFlushAll}
	if len(parts) > 1 {
		delay, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("invalid flush_all delay: %w", err)
		}
		cmd.Exptime = delay
	}
	return cmd, nil
}

func readData(r *bufio.Reader, byteCount int) ([]byte, error) {
	// +2 for trailing \r\n
	buf := make([]byte, byteCount+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read data block: %w", err)
	}
	if buf[byteCount] != '\r' || buf[byteCount+1] != '\n' {
		return nil, errors.New("data block not terminated by \\r\\n")
	}
	return buf[:byteCount], nil
}
