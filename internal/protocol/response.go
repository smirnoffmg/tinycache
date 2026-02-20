package protocol

import (
	"fmt"
	"io"
)

const (
	RespStored    = "STORED\r\n"
	RespNotStored = "NOT_STORED\r\n"
	RespExists    = "EXISTS\r\n"
	RespNotFound  = "NOT_FOUND\r\n"
	RespDeleted   = "DELETED\r\n"
	RespEnd       = "END\r\n"
	RespError     = "ERROR\r\n"
	RespOK        = "OK\r\n"
)

// WriteValue writes a VALUE response line: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n
func WriteValue(w io.Writer, key string, flags uint32, data []byte, casUnique uint64, includeCas bool) error {
	if includeCas {
		if _, err := fmt.Fprintf(w, "VALUE %s %d %d %d\r\n", key, flags, len(data), casUnique); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintf(w, "VALUE %s %d %d\r\n", key, flags, len(data)); err != nil {
			return err
		}
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	_, err := w.Write([]byte("\r\n"))
	return err
}

// WriteServerError writes a SERVER_ERROR response with a message.
func WriteServerError(w io.Writer, msg string) error {
	_, err := fmt.Fprintf(w, "SERVER_ERROR %s\r\n", msg)
	return err
}

// WriteClientError writes a CLIENT_ERROR response with a message.
func WriteClientError(w io.Writer, msg string) error {
	_, err := fmt.Fprintf(w, "CLIENT_ERROR %s\r\n", msg)
	return err
}

// WriteStat writes a single STAT line: STAT <name> <value>\r\n
func WriteStat(w io.Writer, name, value string) error {
	_, err := fmt.Fprintf(w, "STAT %s %s\r\n", name, value)
	return err
}
