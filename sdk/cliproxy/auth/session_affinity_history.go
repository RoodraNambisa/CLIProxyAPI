package auth

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/session"
)

func (s *SessionAffinitySelector) historyRequest(ctx context.Context, provider, model string, opts core.Options) (string, *session.History, bool) {
	if s == nil || !s.lcp || s.historyMatcher == nil {
		return "", nil, false
	}
	captured := captureAffinityIdentity(ctx, core.Request{}, opts, true)
	if captured.scope == "" || captured.identity.SessionID != "" || captured.history == nil || !captured.history.Usable() {
		return "", nil, false
	}
	namespace, _ := json.Marshal([]string{"lcp-affinity-v1", captured.scope, strings.ToLower(strings.TrimSpace(provider)), canonicalModelKey(model)})
	return string(namespace), captured.history, true
}

func (s *SessionAffinitySelector) historyPreferredAuth(ctx context.Context, provider, model string, opts core.Options) string {
	namespace, history, ok := s.historyRequest(ctx, provider, model, opts)
	if !ok {
		return ""
	}
	if match, ok := s.historyMatcher.Match(namespace, *history); ok {
		return match.AuthID
	}
	return ""
}

func (s *SessionAffinitySelector) pickHistoryPreference(ctx context.Context, provider, model string, opts core.Options, auths []*Auth, prepared bool) (*Auth, bool, error) {
	preferred := s.historyPreferredAuth(ctx, provider, model, opts)
	if preferred == "" {
		return nil, false, nil
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, true, ctx.Err()
	}
	available := auths
	if !prepared {
		priorityPreference := ""
		if s.acrossPriorities {
			priorityPreference = preferred
		}
		var err error
		available, err = getAvailableAuthsForContextWithPreference(ctx, auths, provider, model, time.Now(), selectionAttemptFromMetadata(opts.Metadata), priorityPreference)
		if err != nil {
			return nil, true, err
		}
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, true, ctx.Err()
	}
	for _, auth := range available {
		if auth.ID == preferred {
			return auth, true, nil
		}
	}
	return nil, false, nil
}
