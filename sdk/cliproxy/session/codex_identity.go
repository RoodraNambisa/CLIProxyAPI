// Package session extracts bounded identities for optional affinity policies.
package session

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"unicode"

	"github.com/tidwall/gjson"
)

// Identity contains detached protocol identifiers, never authentication data.
type Identity struct {
	SessionID       string
	ParentSessionID string
	IsFork          bool
	IsSubagent      bool
}

func explicitID(raw string) string {
	for _, r := range raw {
		if unicode.IsControl(r) {
			return ""
		}
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > 256 {
		return ""
	}
	return strings.Clone(raw)
}

func stringID(value gjson.Result) string {
	if value.Type != gjson.String {
		return ""
	}
	return explicitID(value.Str)
}

// headerValue also accepts non-canonical SDK headers. Conflicting duplicates
// are not a reliable identity and must not depend on map iteration order.
func headerValue(headers http.Header, name string) string {
	value := ""
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		for _, raw := range values {
			if next := raw; strings.TrimSpace(next) != "" {
				if value != "" && value != next {
					return ""
				}
				value = next
			}
		}
	}
	return value
}

func protocolRoots(payload []byte) []gjson.Result {
	root := gjson.ParseBytes(payload)
	roots := []gjson.Result{root}
	if request := root.Get("request"); request.IsObject() && !root.Get("contents").Exists() {
		roots = append(roots, request)
	}
	return roots
}

func rootID(roots []gjson.Result, paths ...string) string {
	for _, path := range paths {
		for _, root := range roots {
			if value := stringID(root.Get(path)); value != "" {
				return value
			}
		}
	}
	return ""
}

func agentIdentity(sessionID, agent string) string {
	return agentIdentityForClient("codex", sessionID, agent)
}

func agentIdentityForClient(client, sessionID, agent string) string {
	raw, _ := json.Marshal([]string{sessionID, agent})
	digest := sha256.Sum256(raw)
	return client + "-agent:" + hex.EncodeToString(digest[:])
}

// ExtractCodexIdentity reads only known protocol fields. It does not infer a
// relationship from messages, cache keys, or arbitrary tool arguments.
func ExtractCodexIdentity(headers http.Header, payload []byte) (Identity, bool) {
	return extractCodexIdentity(headers, protocolRoots(payload))
}

func extractCodexIdentity(headers http.Header, roots []gjson.Result) (Identity, bool) {
	headers = codexFrameIdentityHeaders(headers, roots)
	sid := explicitID(headerValue(headers, "Session-Id"))
	if sid == "" {
		sid = explicitID(headerValue(headers, "Session_id"))
	}
	tid := explicitID(headerValue(headers, "Thread-Id"))
	if tid == "" {
		tid = explicitID(headerValue(headers, "Thread_id"))
	}
	var turn gjson.Result
	if raw := headerValue(headers, "X-Codex-Turn-Metadata"); len(raw) <= 64*1024 && gjson.Valid(raw) {
		turn = gjson.Parse(raw)
	}
	if sid == "" {
		sid = stringID(turn.Get("session_id"))
	}
	if tid == "" {
		tid = stringID(turn.Get("thread_id"))
	}
	if tid == "" && sid != "" {
		tid = rootID(roots, "thread_id", "threadId", "metadata.thread_id")
	}
	if sid == "" && tid == "" {
		return Identity{}, false
	}
	parent := explicitID(headerValue(headers, "X-Codex-Parent-Thread-Id"))
	if parent == "" {
		parent = stringID(turn.Get("parent_thread_id"))
	}
	if parent == "" {
		parent = rootID(roots, "parent_thread_id", "parentThreadId", "parent_session_id", "parentSessionId", "metadata.parent_thread_id", "metadata.parent_session_id", "extra_body.parent_thread_id", "extra_body.parent_session_id")
	}
	fork := rootID([]gjson.Result{turn}, "forked_from_thread_id", "forked_from_id")
	if fork == "" {
		fork = rootID(roots, "forked_from_thread_id", "forked_from_id", "metadata.forked_from_thread_id", "metadata.forked_from_id", "extra_body.forked_from_thread_id", "extra_body.forked_from_id")
	}
	child := tid
	if child == "" {
		child = sid
	}
	info := Identity{SessionID: "codex:" + child}
	if fork != "" {
		if child == fork && sid != "" && sid != fork {
			info.SessionID = "codex:" + sid
		}
		info.ParentSessionID, info.IsFork = "codex:"+fork, true
	} else {
		sub := explicitID(headerValue(headers, "X-Openai-Subagent"))
		info.IsSubagent = (sub != "" && !strings.EqualFold(sub, "false") && sub != "0") ||
			stringID(turn.Get("subagent_kind")) == "thread_spawn" ||
			(sid != "" && tid != "" && sid != tid) || (parent != "" && parent != child)
		if info.IsSubagent {
			if parent == "" {
				parent = sid
			}
			agent := stringID(turn.Get("agent_name"))
			agent = strings.TrimPrefix(strings.TrimPrefix(agent, "/root/"), "/")
			if agent != "" && agent != "root" && agent != "main" && sid != "" {
				info.SessionID = agentIdentity(sid, agent)
			}
			if parent != "" {
				info.ParentSessionID = "codex:" + parent
			}
		} else if sid != "" {
			info.SessionID = "codex:" + sid
		}
	}
	if info.ParentSessionID == info.SessionID {
		info.ParentSessionID = ""
	}
	return info, true
}
