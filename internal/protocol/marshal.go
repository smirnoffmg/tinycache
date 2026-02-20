package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

// Marshal reconstructs raw Memcached text protocol bytes from a parsed Command.
func Marshal(cmd Command) []byte {
	var b strings.Builder

	switch cmd.Type {
	case CmdGet:
		fmt.Fprintf(&b, "get %s\r\n", strings.Join(cmd.Keys, " "))
	case CmdGets:
		fmt.Fprintf(&b, "gets %s\r\n", strings.Join(cmd.Keys, " "))
	case CmdSet:
		marshalStorage(&b, "set", cmd)
	case CmdAdd:
		marshalStorage(&b, "add", cmd)
	case CmdReplace:
		marshalStorage(&b, "replace", cmd)
	case CmdCas:
		marshalCas(&b, cmd)
	case CmdDelete:
		if cmd.Noreply {
			fmt.Fprintf(&b, "delete %s noreply\r\n", cmd.Keys[0])
		} else {
			fmt.Fprintf(&b, "delete %s\r\n", cmd.Keys[0])
		}
	case CmdFlushAll:
		if cmd.Exptime > 0 {
			fmt.Fprintf(&b, "flush_all %d\r\n", cmd.Exptime)
		} else {
			b.WriteString("flush_all\r\n")
		}
	case CmdStats:
		b.WriteString("stats\r\n")
	case CmdQuit:
		b.WriteString("quit\r\n")
	}

	return []byte(b.String())
}

func marshalStorage(b *strings.Builder, verb string, cmd Command) {
	fmt.Fprintf(b, "%s %s %d %d %d",
		verb, cmd.Keys[0], cmd.Flags, cmd.Exptime, len(cmd.Data))
	if cmd.Noreply {
		b.WriteString(" noreply")
	}
	b.WriteString("\r\n")
	b.Write(cmd.Data)
	b.WriteString("\r\n")
}

func marshalCas(b *strings.Builder, cmd Command) {
	fmt.Fprintf(b, "cas %s %d %d %d %s",
		cmd.Keys[0], cmd.Flags, cmd.Exptime, len(cmd.Data),
		strconv.FormatUint(cmd.CasUnique, 10))
	if cmd.Noreply {
		b.WriteString(" noreply")
	}
	b.WriteString("\r\n")
	b.Write(cmd.Data)
	b.WriteString("\r\n")
}
