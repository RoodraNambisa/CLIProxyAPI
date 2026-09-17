package auth

import "testing"

func TestProxyPreviewUsesLinkedSourceAndFailsClosedOnIdentityChange(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	updated := source.Clone()
	updated.ProxyURL = "socks5h://source.example:1080"
	if _, err := manager.Update(WithSkipPersist(t.Context()), updated); err != nil {
		t.Fatal(err)
	}
	web.ProxyURL = "http://ignored-web.example"
	preview, err := manager.PreviewProxyAuth(web)
	if err != nil || !preview.Linked || preview.URL != updated.ProxyURL {
		t.Fatalf("incorrect linked preview: %+v %v", preview, err)
	}
	updated.Metadata["account_id"] = "other-account"
	if _, err := manager.Update(WithSkipPersist(t.Context()), updated); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.PreviewProxyAuth(web); err == nil {
		t.Fatal("identity change was ignored")
	}
}
