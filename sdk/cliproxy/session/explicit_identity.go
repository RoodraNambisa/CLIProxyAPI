package session

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
)

var claudeSessionSuffix = regexp.MustCompile(`_session_([a-f0-9-]+)$`)

// ExtractExplicitIdentity preserves native client signals ahead of generic
// hints. Cache keys remain a distinct role and never establish parentage.
func ExtractExplicitIdentity(headers http.Header, payload []byte, executionID string) (Identity, bool) {
	roots := protocolRoots(payload)
	if identity, ok := extractClaudeIdentity(headers, roots); ok {
		return identity, true
	}
	if identity, ok := extractCodexIdentity(headers, roots); ok {
		return identity, true
	}
	for _, field := range [][3]string{
		{"X-Http-Session-Id", "X-Parent-Session-ID", "agy"},
		{"X-Session-ID", "X-Parent-Session-ID", "header"},
		{"X-Session-Affinity", "X-Parent-Session-Affinity", "affinity"},
		{"X-Slot-Session-Id", "X-Parent-Session-ID", "slot"},
		{"X-Conversation-Id", "X-Parent-Conversation-Id", "conv"},
		{"X-Thread-Id", "X-Parent-Thread-Id", "thread"},
		{"X-Client-Request-Id", "", "clientreq"},
	} {
		if id := explicitID(headerValue(headers, field[0])); id != "" {
			return identityWithParent(field[2], id, explicitID(headerValue(headers, field[1])), roots), true
		}
	}
	for _, field := range [][2]string{
		{"thread_id", "thread"}, {"threadId", "thread"}, {"metadata.thread_id", "thread"},
		{"session_id", "session"}, {"sessionId", "session"}, {"metadata.session_id", "session"},
		{"conversation.id", "conv"}, {"conversation_id", "conv"}, {"conversationId", "conv"},
		{"chat_id", "conv"}, {"chatId", "conv"}, {"metadata.conversation_id", "conv"},
		{"extra_body.conversation_id", "conv"}, {"metadata.user_id", "user"},
	} {
		if id := rootID(roots, field[0]); id != "" {
			return identityWithParent(field[1], id, "", roots), true
		}
	}
	if id := promptCacheIdentity(roots[0].Get("prompt_cache_key")); id != "" {
		return Identity{SessionID: id}, true
	}
	if id := explicitID(executionID); id != "" {
		return Identity{SessionID: "execution:" + id}, true
	}
	return Identity{}, false
}

func identityWithParent(client, id, parent string, roots []gjson.Result) Identity {
	info := Identity{SessionID: client + ":" + id}
	if fork := rootID(roots, "forked_from_thread_id", "forked_from_id", "metadata.forked_from_thread_id", "metadata.forked_from_id", "extra_body.forked_from_thread_id", "extra_body.forked_from_id"); fork != "" {
		parent, info.IsFork = fork, true
	} else if parent == "" {
		parent = rootID(roots, "parent_session_id", "parentSessionId", "parent_thread_id", "parentThreadId", "parent_conversation_id", "parentConversationId", "metadata.parent_session_id", "metadata.parent_thread_id", "extra_body.parent_session_id", "extra_body.parent_thread_id")
	}
	if parent != "" && parent != id {
		info.ParentSessionID = client + ":" + parent
		info.IsSubagent = !info.IsFork
	}
	return info
}

func extractClaudeIdentity(headers http.Header, roots []gjson.Result) (Identity, bool) {
	sid := explicitID(headerValue(headers, "X-Claude-Code-Session-Id"))
	var metadataSID, parentSID, agent string
	for _, root := range roots {
		value := root.Get("metadata.user_id")
		if value.Type != gjson.String {
			continue
		}
		raw := strings.TrimSpace(value.Str)
		if strings.HasPrefix(raw, "{") && len(raw) <= 64*1024 && gjson.Valid(raw) {
			parsed := gjson.Parse(raw)
			metadataSID = stringID(parsed.Get("session_id"))
			parentSID = stringID(parsed.Get("parent_session_id"))
			agent = stringID(parsed.Get("agent_id"))
		} else if matches := claudeSessionSuffix.FindStringSubmatch(raw); len(matches) == 2 {
			metadataSID = explicitID(matches[1])
		}
		if metadataSID != "" {
			break
		}
	}
	if sid == "" {
		sid = metadataSID
	}
	if sid == "" {
		return Identity{}, false
	}
	if headerAgent := explicitID(headerValue(headers, "X-Claude-Code-Agent-Id")); headerAgent != "" {
		agent = headerAgent
	}
	if agent == "" {
		agent = rootID(roots, "metadata.agent_id", "metadata.subagent_id")
	}
	info := identityWithParent("claude", sid, parentSID, roots)
	if agent != "" && agent != "main" && !info.IsFork {
		info.SessionID = agentIdentityForClient("claude", sid, agent)
		info.IsSubagent = true
		if parentAgent := explicitID(headerValue(headers, "X-Claude-Code-Parent-Agent-Id")); parentAgent != "" && parentAgent != "main" && parentAgent != agent {
			info.ParentSessionID = agentIdentityForClient("claude", sid, parentAgent)
		} else if info.ParentSessionID == "" {
			info.ParentSessionID = "claude:" + sid
		}
	}
	return info, true
}
