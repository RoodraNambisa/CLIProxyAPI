package main

import "testing"

func TestModelsUpdaterStartupModes(t *testing.T) {
	for _, mode := range []struct {
		name                        string
		tui, standalone, wantRemote bool
	}{
		{"ordinary", false, false, true},
		{"ordinary with standalone flag", false, true, true},
		{"standalone TUI", true, true, true},
		{"management TUI only", true, false, false},
	} {
		t.Run(mode.name, func(t *testing.T) {
			if got := shouldStartModelsUpdater(false, mode.tui, mode.standalone); got != mode.wantRemote {
				t.Fatalf("remote refresh=%t, want %t", got, mode.wantRemote)
			}
			if shouldStartModelsUpdater(true, mode.tui, mode.standalone) {
				t.Fatal("--local-model allowed remote refresh")
			}
		})
	}
}
