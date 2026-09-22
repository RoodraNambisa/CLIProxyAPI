package helps

import (
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"testing"
)

type cookieProbeRoundTrip func(*http.Request) (*http.Response, error)

func (f cookieProbeRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestCookieAcquisitionDisablesJarAndStripsEveryAttempt(t *testing.T) {
	u, _ := url.Parse("https://chatgpt.com/backend-api/codex/responses")
	jar, _ := cookiejar.New(nil)
	jar.SetCookies(u, []*http.Cookie{{Name: "__oailb", Value: "jar", Path: "/"}})
	calls := 0
	original := &http.Client{Jar: jar, Transport: cookieProbeRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		for k := range r.Header {
			if strings.EqualFold(k, "Cookie") || strings.EqualFold(k, "X-Codex-Turn-State") {
				t.Fatalf("acquisition leaked %s", k)
			}
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Set-Cookie": {"__oailb=new; Path=/"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}
	client := StateProbeHTTPClient(WithStateProbe(t.Context()), original)
	for range 2 {
		req, _ := http.NewRequest("POST", u.String(), nil)
		req.Header = http.Header{"cookie": {"__oailb=custom"}, "X-Codex-Turn-State": {"state"}}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if req.Header["cookie"][0] != "__oailb=custom" {
			t.Fatal("mutated caller header snapshot")
		}
	}
	if calls != 2 || original.Jar != jar || jar.Cookies(u)[0].Value != "jar" {
		t.Fatal("acquisition changed shared cookie jar")
	}
}
