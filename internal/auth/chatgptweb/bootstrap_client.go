package chatgptweb

import (
	"context"
	"fmt"
)

// NewBootstrapAttempt shares browser identity, cookies and first-request
// accounting, but never shares connections with generation or other attempts.
// The caller must close active and idle connections when the attempt completes.
func (client *Client) NewBootstrapAttempt(ctx context.Context) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("browser client is nil")
	}
	profile, ok := findTLSProfile(client.persona.Profile)
	if !ok {
		return nil, fmt.Errorf("unsupported TLS profile %q", client.persona.Profile)
	}
	tracker := newConnectionTracker()
	tracker.lifetime = ctx
	transport, err := newBrowserHTTPClient(profile, client.jar, client.proxyURL, 0, false, tracker, false)
	if err != nil {
		return nil, err
	}
	attempt := &Client{
		noRedirect: transport, jar: client.jar, persona: client.persona,
		proxyURL: client.proxyURL, sendSessionCookies: client.sendSessionCookies,
		acquisitionTracker: tracker, beforeRequest: client.runBeforeRequest,
	}
	attempt.accessTokenGuard.Store(client.accessTokenGuard.Load())
	attempt.requestCancelStop = context.AfterFunc(ctx, tracker.closeAll)
	return attempt, nil
}
