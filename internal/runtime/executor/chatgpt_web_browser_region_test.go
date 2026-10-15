package executor

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type regionPreviewOnlyResolver struct{ calls int }

func (r *regionPreviewOnlyResolver) Resolve(context.Context, *coreauth.Auth) (coreauth.ResolvedProxy, error) {
	r.calls++
	return coreauth.ResolvedProxy{}, nil
}
func (*regionPreviewOnlyResolver) ReportFailure(context.Context, *coreauth.Auth, error) error {
	return nil
}
func TestChatGPTWebBrowserRegionPreviewDoesNotAllocate(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	resolver := &regionPreviewOnlyResolver{}
	manager.SetProxyResolver(resolver)
	e := NewChatGPTWebExecutor(&config.Config{}, manager)
	defer func() { _ = e.Close() }()
	auth := chatGPTWebRuntimeAuth()
	for _, refresh := range []bool{false, true} {
		snapshot, code := e.BrowserRegionPreview(auth, refresh)
		if code != "route_unresolved" || snapshot.Reason != "route_unresolved" || resolver.calls != 0 {
			t.Fatalf("preview allocated a route: %s %+v calls=%d", code, snapshot, resolver.calls)
		}
	}
}
