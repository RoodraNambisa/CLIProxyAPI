package management

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
)

type modelProbeStateInput struct {
	Mode  string `json:"mode"`
	State string `json:"x-codex-turn-state,omitempty"`
}

type modelProbeStateResult struct {
	Mode           string `json:"mode"`
	Source         string `json:"source"`
	SentLength     int    `json:"sent_length"`
	SentDigest     string `json:"sent_digest,omitempty"`
	ReturnedLength int    `json:"returned_length"`
	ReturnedDigest string `json:"returned_digest,omitempty"`
	State          string `json:"x-codex-turn-state,omitempty"`
}

func validModelProbeState(value string) bool {
	if value == "" || len(value) > 8192 {
		return false
	}
	for _, b := range []byte(value) {
		if b < 33 || b > 126 {
			return false
		}
	}
	return true
}

func validateModelProbeState(input *modelProbeStateInput, provider string) (string, string, error) {
	if input == nil {
		if provider == "codex" {
			return "auto", "", nil
		}
		return "configured", "", nil
	}
	mode, value := strings.TrimSpace(input.Mode), strings.TrimSpace(input.State)
	if mode == "" {
		mode = "auto"
	}
	if provider != "codex" || !slices.Contains([]string{"auto", "configured", "none", "custom", "managed", "acquired"}, mode) {
		return "", "", errors.New("State probe options require Codex and a supported mode")
	}
	if (mode == "custom" && !validModelProbeState(value)) || (mode != "custom" && value != "") {
		return "", "", errors.New("custom State must contain 1–8192 visible ASCII characters and is accepted only in custom mode")
	}
	return mode, value, nil
}

func modelProbeStateDigest(value string) string {
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

func (t *modelProbeTrace) stateApplied(source, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.codexState == nil {
		return
	}
	t.codexState.Source = source
	t.codexState.SentLength = len(value)
	t.codexState.SentDigest = modelProbeStateDigest(value)
	t.stateSent = value
}

func (t *modelProbeTrace) sentState() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stateSent
}

func (t *modelProbeTrace) returnedState() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.codexState == nil {
		return ""
	}
	return t.codexState.State
}
