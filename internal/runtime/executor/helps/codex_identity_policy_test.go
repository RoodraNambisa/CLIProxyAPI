package helps

import (
	"context"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"net/http"
	"testing"
)

func TestCodexInactiveIdentityConnectionHasNoAllocations(t *testing.T) {
	ctx := context.Background()
	policy := config.CodexIdentityPolicy{FingerprintMode: "off", TurnStatePolicy: config.CodexTurnStatePolicyGuardCrossAccount}
	headers := http.Header{"Session-Id": []string{"client-session"}}
	if allocations := testing.AllocsPerRun(1000, func() {
		if WithCodexIdentityConnection(ctx, policy, headers) != ctx {
			t.Fatal("inactive policy wrapped context")
		}
	}); allocations != 0 {
		t.Fatalf("inactive connection allocated %f", allocations)
	}
	policy.ConfuseEffective = true
	active := WithCodexIdentityConnection(ctx, policy, headers)
	if CodexIdentityConnectionDigest(active) == ([32]byte{}) {
		t.Fatal("active policy lost reuse protection")
	}
}
