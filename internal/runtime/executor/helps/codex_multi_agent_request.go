package helps

// CodexMultiAgentResponsePolicy records which declarations were prepared for one
// upstream attempt. It does not retain either the request body or its headers.
type CodexMultiAgentResponsePolicy struct {
	NamespaceOptimized bool
	PlaintextCalls     bool
	NamespaceConflict  bool
}

// OptimizeCodexMultiAgentV2Request applies transport-specific preparation after
// any Responses boundary model descriptions. Native encrypted history is opaque.
func OptimizeCodexMultiAgentV2Request(payload []byte, policy CodexMultiAgentPolicy, isCompat ...bool) ([]byte, CodexMultiAgentResponsePolicy) {
	if !policy.Enabled {
		return payload, CodexMultiAgentResponsePolicy{}
	}
	scan := scanCodexCollaborationTools(payload)
	responsePolicy := CodexMultiAgentResponsePolicy{
		PlaintextCalls:    len(scan.messagePaths) > 0,
		NamespaceConflict: scan.conflict,
	}
	updated := removeCodexCollaborationMessageEncryption(payload, scan.messagePaths)
	updated, responsePolicy.NamespaceOptimized = OptimizeCodexCollaborationNamespace(updated)
	if len(isCompat) > 0 && isCompat[0] {
		updated = rewriteCodexCompatibilityAgentMessages(updated)
	}
	return updated, responsePolicy
}

func (policy CodexMultiAgentResponsePolicy) Rewrite(payload []byte) []byte {
	return RewriteCodexMultiAgentV2Response(payload, policy.NamespaceOptimized, policy.PlaintextCalls)
}
