// Package xai implements thinking configuration for xAI Grok Responses models.
package xai

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/codex"
)

// Applier uses the Responses API reasoning.effort representation shared with Codex.
type Applier struct {
	codex.Applier
}

var _ thinking.ProviderApplier = (*Applier)(nil)

// NewApplier creates an xAI thinking applier.
func NewApplier() *Applier { return &Applier{} }

func init() {
	thinking.RegisterProvider("xai", NewApplier())
}
