package protocol_test

import (
	"bufio"
	"strings"
	"testing"
	"tinycache/internal/protocol"
)

func reader(s string) *bufio.Reader {
	return bufio.NewReader(strings.NewReader(s))
}

func TestParseGet_SingleKey(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("get foo\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdGet)
	assertSliceEqual(t, cmd.Keys, []string{"foo"})
}

func TestParseGet_MultipleKeys(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("get foo bar baz\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdGet)
	assertSliceEqual(t, cmd.Keys, []string{"foo", "bar", "baz"})
}

func TestParseGets_WithCAS(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("gets foo bar\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdGets)
	assertSliceEqual(t, cmd.Keys, []string{"foo", "bar"})
}

func TestParseSet(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("set foo 0 300 3\r\nbar\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdSet)
	assertSliceEqual(t, cmd.Keys, []string{"foo"})
	assertEqual(t, cmd.Flags, uint32(0))
	assertEqual(t, cmd.Exptime, int64(300))
	assertEqual(t, cmd.Bytes, 3)
	assertEqual(t, string(cmd.Data), "bar")
}

func TestParseSet_WithNoreply(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("set foo 42 0 5 noreply\r\nhello\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdSet)
	assertEqual(t, cmd.Flags, uint32(42))
	assertEqual(t, cmd.Noreply, true)
	assertEqual(t, string(cmd.Data), "hello")
}

func TestParseAdd(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("add mykey 0 0 4\r\ndata\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdAdd)
	assertSliceEqual(t, cmd.Keys, []string{"mykey"})
	assertEqual(t, string(cmd.Data), "data")
}

func TestParseReplace(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("replace k 0 0 2\r\nhi\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdReplace)
	assertEqual(t, string(cmd.Data), "hi")
}

func TestParseCas(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("cas foo 0 0 3 12345\r\nbar\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdCas)
	assertSliceEqual(t, cmd.Keys, []string{"foo"})
	assertEqual(t, cmd.CasUnique, uint64(12345))
	assertEqual(t, string(cmd.Data), "bar")
}

func TestParseCas_WithNoreply(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("cas foo 0 0 3 99 noreply\r\nbar\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdCas)
	assertEqual(t, cmd.CasUnique, uint64(99))
	assertEqual(t, cmd.Noreply, true)
}

func TestParseDelete(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("delete foo\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdDelete)
	assertSliceEqual(t, cmd.Keys, []string{"foo"})
}

func TestParseDelete_WithNoreply(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("delete foo noreply\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdDelete)
	assertEqual(t, cmd.Noreply, true)
}

func TestParseFlushAll(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("flush_all\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdFlushAll)
}

func TestParseFlushAll_WithDelay(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("flush_all 30\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdFlushAll)
	assertEqual(t, cmd.Exptime, int64(30))
}

func TestParseStats(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("stats\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdStats)
}

func TestParseQuit(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("quit\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Type, protocol.CmdQuit)
}

func TestParse_UnknownCommand(t *testing.T) {
	t.Parallel()

	_, err := protocol.Parse(reader("bogus\r\n"))
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
}

func TestParse_GetNoKeys(t *testing.T) {
	t.Parallel()

	_, err := protocol.Parse(reader("get\r\n"))
	if err == nil {
		t.Fatal("expected error for get with no keys")
	}
}

func TestParse_SetMissingArgs(t *testing.T) {
	t.Parallel()

	_, err := protocol.Parse(reader("set foo 0\r\n"))
	if err == nil {
		t.Fatal("expected error for set with missing args")
	}
}

func TestParse_SetWrongByteCount(t *testing.T) {
	t.Parallel()

	_, err := protocol.Parse(reader("set foo 0 0 10\r\nbar\r\n"))
	if err == nil {
		t.Fatal("expected error for set with wrong byte count")
	}
}

func TestParse_DeleteNoKey(t *testing.T) {
	t.Parallel()

	_, err := protocol.Parse(reader("delete\r\n"))
	if err == nil {
		t.Fatal("expected error for delete with no key")
	}
}

func TestParse_EmptyInput(t *testing.T) {
	t.Parallel()

	_, err := protocol.Parse(reader(""))
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestParse_SetZeroBytes(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("set foo 0 0 0\r\n\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Bytes, 0)
	assertEqual(t, len(cmd.Data), 0)
}

func TestParse_BinaryData(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("set foo 0 0 4\r\n\x00\x01\x02\x03\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, len(cmd.Data), 4)
	assertEqual(t, cmd.Data[0], byte(0))
	assertEqual(t, cmd.Data[3], byte(3))
}

func TestParse_BinaryValueWithNullBytes(t *testing.T) {
	t.Parallel()

	data := "set bin 0 0 6\r\n\x00\xff\r\n\x00\x01\r\n"
	cmd, err := protocol.Parse(reader(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, len(cmd.Data), 6)
	assertEqual(t, cmd.Data[0], byte(0x00))
	assertEqual(t, cmd.Data[1], byte(0xff))
	assertEqual(t, cmd.Data[2], byte('\r'))
	assertEqual(t, cmd.Data[3], byte('\n'))
	assertEqual(t, cmd.Data[4], byte(0x00))
	assertEqual(t, cmd.Data[5], byte(0x01))
}

func TestParse_MultipleCommandsInSequence(t *testing.T) {
	t.Parallel()

	input := "set a 0 0 1\r\nx\r\nget a\r\ndelete a\r\n"
	r := reader(input)

	cmd1, err := protocol.Parse(r)
	if err != nil {
		t.Fatalf("cmd1: %v", err)
	}
	assertEqual(t, cmd1.Type, protocol.CmdSet)

	cmd2, err := protocol.Parse(r)
	if err != nil {
		t.Fatalf("cmd2: %v", err)
	}
	assertEqual(t, cmd2.Type, protocol.CmdGet)

	cmd3, err := protocol.Parse(r)
	if err != nil {
		t.Fatalf("cmd3: %v", err)
	}
	assertEqual(t, cmd3.Type, protocol.CmdDelete)
}

func TestParse_LargeFlags(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("set k 4294967295 0 1\r\nx\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Flags, uint32(4294967295))
}

func TestParse_NegativeExptime(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("set k 0 -1 1\r\nx\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Exptime, int64(-1))
}

func TestParse_AbsoluteExptime(t *testing.T) {
	t.Parallel()

	cmd, err := protocol.Parse(reader("set k 0 2000000000 1\r\nx\r\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, cmd.Exptime, int64(2000000000))
}

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func assertSliceEqual[T comparable](t *testing.T, got, want []T) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got len %d, want len %d", len(got), len(want))
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("index %d: got %v, want %v", i, got[i], want[i])
		}
	}
}
