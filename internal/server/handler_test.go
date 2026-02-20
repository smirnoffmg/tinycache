package server_test

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"tinycache/internal/cache"
	"tinycache/internal/server"
)

func setup() (*server.Handler, *cache.Store) {
	s := cache.NewStore(256, 0)
	h := server.NewHandler(s)
	return h, s
}

func exec(t *testing.T, h *server.Handler, input string) string {
	t.Helper()
	r := bufio.NewReader(strings.NewReader(input))
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	h.HandleCommand(r, w)
	w.Flush()
	return buf.String()
}

func TestHandler_Set(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	resp := exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	assertEqual(t, resp, "STORED\r\n")
}

func TestHandler_Get_Hit(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	resp := exec(t, h, "get foo\r\n")

	if !strings.Contains(resp, "VALUE foo 0 3") {
		t.Errorf("expected VALUE line, got %q", resp)
	}
	if !strings.Contains(resp, "bar") {
		t.Errorf("expected data 'bar', got %q", resp)
	}
	if !strings.HasSuffix(resp, "END\r\n") {
		t.Errorf("expected END suffix, got %q", resp)
	}
}

func TestHandler_Get_Miss(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	resp := exec(t, h, "get missing\r\n")
	assertEqual(t, resp, "END\r\n")
}

func TestHandler_Gets_IncludesCAS(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	resp := exec(t, h, "gets foo\r\n")

	parts := strings.Fields(strings.Split(resp, "\r\n")[0])
	if len(parts) < 5 {
		t.Fatalf("expected VALUE with CAS token, got %q", resp)
	}
	if parts[0] != "VALUE" || parts[1] != "foo" {
		t.Errorf("unexpected VALUE line: %q", resp)
	}
}

func TestHandler_Add_NewKey(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	resp := exec(t, h, "add foo 0 0 3\r\nbar\r\n")
	assertEqual(t, resp, "STORED\r\n")
}

func TestHandler_Add_ExistingKey(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	resp := exec(t, h, "add foo 0 0 3\r\nbaz\r\n")
	assertEqual(t, resp, "NOT_STORED\r\n")
}

func TestHandler_Replace_ExistingKey(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	resp := exec(t, h, "replace foo 0 0 3\r\nbaz\r\n")
	assertEqual(t, resp, "STORED\r\n")
}

func TestHandler_Replace_MissingKey(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	resp := exec(t, h, "replace foo 0 0 3\r\nbar\r\n")
	assertEqual(t, resp, "NOT_STORED\r\n")
}

func TestHandler_Delete_ExistingKey(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	resp := exec(t, h, "delete foo\r\n")
	assertEqual(t, resp, "DELETED\r\n")
}

func TestHandler_Delete_MissingKey(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	resp := exec(t, h, "delete missing\r\n")
	assertEqual(t, resp, "NOT_FOUND\r\n")
}

func TestHandler_FlushAll(t *testing.T) {
	t.Parallel()
	h, s := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	resp := exec(t, h, "flush_all\r\n")
	assertEqual(t, resp, "OK\r\n")
	assertEqual(t, s.Len(), 0)
}

func TestHandler_Stats(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	resp := exec(t, h, "stats\r\n")

	if !strings.Contains(resp, "STAT curr_items") {
		t.Errorf("expected STAT curr_items, got %q", resp)
	}
	if !strings.HasSuffix(resp, "END\r\n") {
		t.Errorf("expected END suffix, got %q", resp)
	}
}

func TestHandler_UnknownCommand(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	resp := exec(t, h, "bogus\r\n")
	if !strings.HasPrefix(resp, "ERROR") {
		t.Errorf("expected ERROR response, got %q", resp)
	}
}

func TestHandler_SetNoreply(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	resp := exec(t, h, "set foo 0 0 3 noreply\r\nbar\r\n")
	assertEqual(t, resp, "")
}

func TestHandler_Cas_Success(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	getsResp := exec(t, h, "gets foo\r\n")

	// Extract CAS token from VALUE line
	parts := strings.Fields(strings.Split(getsResp, "\r\n")[0])
	casToken := parts[4]

	resp := exec(t, h, "cas foo 0 0 3 "+casToken+"\r\nbaz\r\n")
	assertEqual(t, resp, "STORED\r\n")
}

func TestHandler_Cas_Mismatch(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set foo 0 0 3\r\nbar\r\n")
	resp := exec(t, h, "cas foo 0 0 3 99999\r\nbaz\r\n")
	assertEqual(t, resp, "EXISTS\r\n")
}

func TestHandler_Cas_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	resp := exec(t, h, "cas missing 0 0 3 1\r\nbar\r\n")
	assertEqual(t, resp, "NOT_FOUND\r\n")
}

func TestHandler_GetMultipleKeys(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set a 0 0 1\r\n1\r\n")
	exec(t, h, "set b 0 0 1\r\n2\r\n")

	resp := exec(t, h, "get a b\r\n")

	if !strings.Contains(resp, "VALUE a") {
		t.Errorf("expected VALUE a, got %q", resp)
	}
	if !strings.Contains(resp, "VALUE b") {
		t.Errorf("expected VALUE b, got %q", resp)
	}
	if !strings.HasSuffix(resp, "END\r\n") {
		t.Errorf("expected END suffix, got %q", resp)
	}
}

func TestHandler_BinarySafeRoundTrip(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set bin 0 0 6\r\n\x00\xff\r\n\x00\x01\r\n")
	resp := exec(t, h, "get bin\r\n")

	if !strings.Contains(resp, "VALUE bin 0 6") {
		t.Errorf("expected VALUE bin 0 6, got %q", resp)
	}
	if !strings.HasSuffix(resp, "END\r\n") {
		t.Errorf("expected END suffix, got %q", resp)
	}
}

func TestHandler_GetMultipleKeys_PartialHit(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	exec(t, h, "set a 0 0 1\r\n1\r\n")

	resp := exec(t, h, "get a missing_key b\r\n")

	if !strings.Contains(resp, "VALUE a") {
		t.Errorf("expected VALUE a, got %q", resp)
	}
	if strings.Contains(resp, "VALUE missing_key") {
		t.Error("should not include missing key in response")
	}
	if strings.Contains(resp, "VALUE b") {
		t.Error("should not include unset key b in response")
	}
	if !strings.HasSuffix(resp, "END\r\n") {
		t.Errorf("expected END suffix, got %q", resp)
	}
}

func TestHandler_MalformedCommand_ThenValid(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	input := "bogus_garbage\r\nset foo 0 0 3\r\nbar\r\n"
	r := bufio.NewReader(strings.NewReader(input))
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	keepGoing := h.HandleCommand(r, w)
	_ = w.Flush()
	if !keepGoing {
		t.Error("handler should keep connection open after parse error")
	}
	if !strings.HasPrefix(buf.String(), "ERROR") {
		t.Errorf("expected ERROR for bogus command, got %q", buf.String())
	}

	buf.Reset()
	w.Reset(&buf)
	keepGoing = h.HandleCommand(r, w)
	_ = w.Flush()
	if !keepGoing {
		t.Error("handler should keep connection open after valid command")
	}
	assertEqual(t, buf.String(), "STORED\r\n")
}

func TestHandler_Quit_ReturnsFalse(t *testing.T) {
	t.Parallel()
	h, _ := setup()

	r := bufio.NewReader(strings.NewReader("quit\r\n"))
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)

	keepGoing := h.HandleCommand(r, w)
	_ = w.Flush()
	if keepGoing {
		t.Error("handler should return false for quit")
	}
}

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
