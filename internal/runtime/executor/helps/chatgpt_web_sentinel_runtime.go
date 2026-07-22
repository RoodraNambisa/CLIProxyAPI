package helps

import (
	"context"
	"errors"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

// ChatGPTWebSentinelObserver is the request-scoped Session Observer handle.
type ChatGPTWebSentinelObserver interface {
	Snapshot(context.Context) (string, error)
	Close()
}

// ChatGPTWebSentinelRuntime isolates executor orchestration from the SDK runtime.
type ChatGPTWebSentinelRuntime interface {
	Close()
	UpdateConfig(chatgptwebauth.SentinelRuntimeConfig)
	Snapshot() chatgptwebauth.SentinelRuntimeSnapshot
	BeginObserver(context.Context, chatgptwebauth.SentinelSDKRequest) (ChatGPTWebSentinelObserver, error)
	SolveTurnstile(context.Context, chatgptwebauth.ConversationTurnstileSolveRequest, chatgptwebauth.SentinelSDKRequest, ChatGPTWebSentinelObserver) (string, error)
}

type chatGPTWebSentinelRuntime struct {
	manager *chatgptwebauth.SentinelRuntimeManager
}

// NewChatGPTWebSentinelRuntime creates the production Sentinel runtime adapter.
func NewChatGPTWebSentinelRuntime(config chatgptwebauth.SentinelRuntimeConfig) ChatGPTWebSentinelRuntime {
	return &chatGPTWebSentinelRuntime{manager: chatgptwebauth.NewSentinelRuntimeManager(config)}
}

func (runtime *chatGPTWebSentinelRuntime) Close() {
	if runtime != nil && runtime.manager != nil {
		runtime.manager.Close()
	}
}

func (runtime *chatGPTWebSentinelRuntime) UpdateConfig(config chatgptwebauth.SentinelRuntimeConfig) {
	if runtime != nil && runtime.manager != nil {
		runtime.manager.UpdateConfig(config)
	}
}

func (runtime *chatGPTWebSentinelRuntime) Snapshot() chatgptwebauth.SentinelRuntimeSnapshot {
	if runtime == nil || runtime.manager == nil {
		return chatgptwebauth.SentinelRuntimeSnapshot{}
	}
	return runtime.manager.Snapshot()
}

func (runtime *chatGPTWebSentinelRuntime) BeginObserver(ctx context.Context, request chatgptwebauth.SentinelSDKRequest) (ChatGPTWebSentinelObserver, error) {
	if runtime == nil || runtime.manager == nil {
		return nil, nil
	}
	return runtime.manager.BeginObserver(ctx, request)
}

func (runtime *chatGPTWebSentinelRuntime) SolveTurnstile(
	ctx context.Context,
	goRequest chatgptwebauth.ConversationTurnstileSolveRequest,
	sdkRequest chatgptwebauth.SentinelSDKRequest,
	observer ChatGPTWebSentinelObserver,
) (string, error) {
	if runtime == nil || runtime.manager == nil {
		return chatgptwebauth.BuildConversationTurnstileTokenWithEnvironment(
			ctx,
			goRequest.DX,
			goRequest.RequirementsToken,
			goRequest.Environment,
			goRequest.Reader,
			goRequest.Now,
		)
	}
	var nativeObserver *chatgptwebauth.SentinelObserver
	if observer != nil {
		var ok bool
		nativeObserver, ok = observer.(*chatgptwebauth.SentinelObserver)
		if !ok {
			return "", errors.New("invalid ChatGPT Web Sentinel observer")
		}
	}
	return runtime.manager.SolveTurnstile(ctx, goRequest, sdkRequest, nativeObserver)
}
