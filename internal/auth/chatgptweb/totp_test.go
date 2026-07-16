package chatgptweb

import (
	"testing"
	"time"
)

func TestTOTPStandardVectors(t *testing.T) {
	t.Parallel()
	const secret = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
	tests := []struct {
		unix int64
		want string
	}{
		{unix: 59, want: "94287082"},
		{unix: 1_111_111_109, want: "07081804"},
		{unix: 1_111_111_111, want: "14050471"},
		{unix: 1_234_567_890, want: "89005924"},
		{unix: 2_000_000_000, want: "69279037"},
		{unix: 20_000_000_000, want: "65353130"},
	}
	for _, test := range tests {
		got, err := generateTOTP(secret, time.Unix(test.unix, 0), 8)
		if err != nil {
			t.Fatalf("generateTOTP(%d): %v", test.unix, err)
		}
		if got != test.want {
			t.Errorf("generateTOTP(%d) = %q, want %q", test.unix, got, test.want)
		}
	}
}

func TestGenerateTOTPSixDigitsAtTime(t *testing.T) {
	t.Parallel()
	got, err := GenerateTOTP("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ", time.Unix(59, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got != "287082" {
		t.Fatalf("GenerateTOTP() = %q, want %q", got, "287082")
	}
}
