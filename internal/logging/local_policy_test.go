package logging

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type localPolicyTestError string

func (e localPolicyTestError) Error() string             { return "rate limit exceeded" }
func (e localPolicyTestError) LocalPolicyReason() string { return string(e) }

func TestLocalPolicyReasonRequiresExplicitSafeMetadata(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"same upstream text", errors.New("rate limit exceeded"), ""},
		{"wrapped", fmt.Errorf("execution: %w", localPolicyTestError("disabled_image_generation_tool")), "disabled_image_generation_tool"},
		{"empty", localPolicyTestError(""), ""},
		{"unsafe", localPolicyTestError("private\nvalue"), ""},
		{"unbounded", localPolicyTestError(strings.Repeat("a", 65)), ""},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := LocalPolicyReason(testCase.err); got != testCase.want {
				t.Fatalf("reason = %q, want %q", got, testCase.want)
			}
		})
	}
}
