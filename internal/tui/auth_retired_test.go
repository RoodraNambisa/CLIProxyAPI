package tui

import (
	"testing"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
)

func TestIsRetiredAuthFile(t *testing.T) {
	tests := []struct {
		name string
		file map[string]any
		want bool
	}{
		{name: "retired flag", file: map[string]any{"retired": true}, want: true},
		{name: "unsupported flag", file: map[string]any{"unsupported": true}, want: true},
		{name: "runtime ineligible", file: map[string]any{"runtime_eligible": false}, want: true},
		{name: "support status", file: map[string]any{"support_status": "unsupported"}, want: true},
		{name: "runtime eligible", file: map[string]any{"runtime_eligible": true}},
		{name: "normal", file: map[string]any{"status": "active"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isRetiredAuthFile(test.file); got != test.want {
				t.Fatalf("isRetiredAuthFile() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestRetiredAuthFileBlocksMutations(t *testing.T) {
	model := newAuthTabModel(nil)
	model.viewport = viewport.New(80, 20)
	model.files = []map[string]any{{"name": "legacy.json", "retired": true}}

	updated, command := model.handleNormalInput(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	if command != nil {
		t.Fatal("toggle command is not nil for retired auth file")
	}
	if updated.status == "" {
		t.Fatal("toggle did not report read-only status")
	}

	if command = updated.startEdit(0); command != nil {
		t.Fatal("edit command is not nil for retired auth file")
	}
	if updated.editing {
		t.Fatal("retired auth file entered edit mode")
	}
}
