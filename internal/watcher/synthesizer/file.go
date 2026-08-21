package synthesizer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// FileSynthesizer generates Auth entries from OAuth JSON files.
// It handles file-based authentication.
type FileSynthesizer struct{}

// NewFileSynthesizer creates a new FileSynthesizer instance.
func NewFileSynthesizer() *FileSynthesizer {
	return &FileSynthesizer{}
}

// Synthesize generates Auth entries from auth files in the auth directory.
func (s *FileSynthesizer) Synthesize(ctx *SynthesisContext) ([]*coreauth.Auth, error) {
	out := make([]*coreauth.Auth, 0, 16)
	if ctx == nil || ctx.AuthDir == "" {
		return out, nil
	}

	entries, err := os.ReadDir(ctx.AuthDir)
	if err != nil {
		// Not an error if directory doesn't exist
		return out, nil
	}

	for _, e := range entries {
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		full := filepath.Join(ctx.AuthDir, name)
		data, errRead := sdkAuth.ReadAuthFileSnapshot(full)
		if errRead != nil || len(data) == 0 {
			continue
		}
		auths := synthesizeFileAuths(ctx, full, data)
		if len(auths) == 0 {
			continue
		}
		out = append(out, auths...)
	}
	return out, nil
}

// SynthesizeAuthFile generates Auth entries for one auth JSON file payload.
// It shares exactly the same mapping behavior as FileSynthesizer.Synthesize.
func SynthesizeAuthFile(ctx *SynthesisContext, fullPath string, data []byte) []*coreauth.Auth {
	if info, errInfo := os.Lstat(fullPath); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	return synthesizeFileAuths(ctx, fullPath, data)
}

func synthesizeFileAuths(ctx *SynthesisContext, fullPath string, data []byte) []*coreauth.Auth {
	if ctx == nil || len(data) == 0 {
		return nil
	}
	var metadata map[string]any
	if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
		return nil
	}
	auths, _ := SynthesizeParsedAuthFile(ctx, fullPath, metadata)
	return auths
}

// SynthesizeParsedAuthFile projects a file snapshot that was already decoded
// after a caller completed its own path and file-generation checks.
func SynthesizeParsedAuthFile(ctx *SynthesisContext, fullPath string, metadata map[string]any) ([]*coreauth.Auth, bool) {
	if ctx == nil || metadata == nil {
		return nil, false
	}
	t, _ := metadata["type"].(string)
	provider := strings.ToLower(strings.TrimSpace(t))
	a := &coreauth.Auth{
		Provider: provider,
		Metadata: metadata,
	}
	if coreauth.IsRetiredGeminiCLIAuth(a) {
		coreauth.WarnRetiredGeminiCLIAuthIgnored()
		return nil, true
	}
	if provider == "" {
		return nil, false
	}
	if errProjection := coreauth.ApplyFileAuthProjection(a, coreauth.FileAuthProjectionOptions{
		Config:  ctx.Config,
		AuthDir: ctx.AuthDir,
		Path:    fullPath,
		Now:     ctx.Now,
	}); errProjection != nil {
		return nil, false
	}
	return []*coreauth.Auth{a}, false
}
