package management

import (
	"errors"
	"slices"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
)

type modelProbeCookieInput struct {
	Mode string `json:"mode"`
}
type modelProbeCookieResult struct {
	Mode    string   `json:"mode"`
	Source  string   `json:"source"`
	Sent    bool     `json:"sent"`
	Digest  string   `json:"digest,omitempty"`
	Version uint64   `json:"version,omitempty"`
	Names   []string `json:"names"`
}

func validateModelProbeCookie(input *modelProbeCookieInput, provider, stateMode string) (string, error) {
	if input == nil {
		return "configured", nil
	}
	mode := strings.TrimSpace(input.Mode)
	if mode == "" {
		mode = "configured"
	}
	if provider != "codex" || !slices.Contains([]string{"configured", "none", "managed", "candidate"}, mode) {
		return "", errors.New("Cookie probe options require Codex and a supported mode")
	}
	if (mode == "managed" || mode == "candidate") && !slices.Contains([]string{"auto", "none"}, stateMode) {
		return "", errors.New("Cookie-only diagnostics cannot send a custom or managed State")
	}
	return mode, nil
}
func (t *modelProbeTrace) cookieApplied(source string, selection codexstate.CookieSelection) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.codexCookie == nil {
		return
	}
	t.codexCookie.Source = source
	t.codexCookie.Sent = selection.Header != ""
	t.codexCookie.Version = selection.Version
	if selection.Header != "" {
		t.codexCookie.Digest = modelProbeStateDigest(selection.Header)
	}
	names := []string{}
	for _, pair := range strings.Split(selection.Header, ";") {
		name, _, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok {
			names = append(names, name)
		}
	}
	t.codexCookie.Names = names
}
