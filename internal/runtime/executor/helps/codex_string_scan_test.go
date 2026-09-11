package helps

import (
	"strings"
	"testing"
)

// originalCodexStringScan is the prior byte-by-byte scanner, used as a parity
// oracle for raw scanning (which intentionally does not validate JSON).
func originalCodexStringScan(payload []byte, start int) (int, bool) {
	if start >= len(payload) || payload[start] != '"' {
		return start, false
	}
	for index := start + 1; index < len(payload); index++ {
		switch payload[index] {
		case '\\':
			index++
			if index >= len(payload) {
				return index, false
			}
		case '"':
			return index + 1, true
		}
	}
	return len(payload), false
}

func TestCodexStringScanBackslashAndTruncationParity(t *testing.T) {
	for slashes := 0; slashes <= 32; slashes++ {
		for _, suffix := range []string{"", `"`, `"tail"`, "\x00\n\xff"} {
			payload := []byte(`{"field":"` + strings.Repeat(`\`, slashes) + suffix)
			for start := range len(payload) + 1 {
				wantEnd, wantOK := originalCodexStringScan(payload, start)
				end, ok := scanCodexJSONString(payload, start)
				if end != wantEnd || ok != wantOK {
					t.Fatalf("scan differs for %q at %d: got %d/%t want %d/%t", payload, start, end, ok, wantEnd, wantOK)
				}
			}
		}
	}
}

func FuzzCodexStringScanPreservesRawBoundaries(f *testing.F) {
	for _, payload := range []string{`"plain"`, `"a\"b\\"tail`, `"a\\\"b"`, `"unterminated\`, "\"\x00\xff\""} {
		f.Add([]byte(payload), uint32(0))
	}
	f.Fuzz(func(t *testing.T, payload []byte, offset uint32) {
		if len(payload) > 1<<20 {
			return
		}
		start := int(offset % uint32(len(payload)+1))
		wantEnd, wantOK := originalCodexStringScan(payload, start)
		end, ok := scanCodexJSONString(payload, start)
		if end != wantEnd || ok != wantOK {
			t.Fatalf("scan differs for %q at %d: got %d/%t want %d/%t", payload, start, end, ok, wantEnd, wantOK)
		}
	})
}
