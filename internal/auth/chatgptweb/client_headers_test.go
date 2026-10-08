package chatgptweb

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	tls_client "github.com/bogdanfinn/tls-client"
)

func TestClientHeaderOverridesReplaceDefaultsOnWire(t *testing.T) {
	for _, useHTTP2 := range []bool{false, true} {
		name := "http1"
		if useHTTP2 {
			name = "http2"
		}
		t.Run(name, func(t *testing.T) {
			observed := make(chan http.Header, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if (r.ProtoMajor == 2) != useHTTP2 {
					t.Errorf("protocol = %s", r.Proto)
				}
				observed <- r.Header.Clone()
				w.WriteHeader(http.StatusNoContent)
			}))
			if useHTTP2 {
				server.EnableHTTP2 = true
				server.StartTLS()
			} else {
				server.Start()
			}
			defer server.Close()
			client, err := NewClient(DefaultPersona(), "", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			if useHTTP2 {
				profile, _ := findTLSProfile(client.persona.Profile)
				// Certificate bypass is limited to this local TLS fixture.
				transport, errTransport := tls_client.NewHttpClient(tls_client.NewNoopLogger(),
					tls_client.WithClientProfile(profile), tls_client.WithCookieJar(client.jar),
					tls_client.WithTimeoutMilliseconds(0), tls_client.WithInsecureSkipVerify())
				if errTransport != nil {
					t.Fatal(errTransport)
				}
				client.noRedirect.CloseIdleConnections()
				client.noRedirect = transport
			}
			overrides := map[string]string{
				"accept": "text/html,application/xhtml+xml", "Accept-Language": "en-GB,en;q=0.9",
				"User-Agent": "header-override-fixture", "Cache-Control": "max-age=0",
				"sec-fetch-mode": "navigate", "sec-fetch-dest": "document",
				"sec-fetch-user": "?1", "upgrade-insecure-requests": "1",
			}
			response, errRequest := client.DoNoRedirectStream(t.Context(), http.MethodGet, server.URL, overrides, nil)
			if errRequest != nil {
				t.Fatal(errRequest)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			headers := <-observed
			if values := headers.Values("DNT"); !reflect.DeepEqual(values, []string{"1"}) {
				t.Errorf("navigation DNT = %q, want exactly one default value", values)
			}
			for key, value := range overrides {
				if values := headers.Values(key); !reflect.DeepEqual(values, []string{value}) {
					t.Errorf("wire %s = %q, want only %q", key, values, value)
				}
			}
			// Reuse the same client for API traffic; document overrides must not
			// leak into later JSON or streaming requests.
			for _, accept := range []string{"application/json", "text/event-stream"} {
				apiHeaders := map[string]string{
					"Accept": accept, "sec-fetch-mode": "cors", "sec-fetch-dest": "empty",
					"Authorization": "Bearer fixture-token",
				}
				response, errRequest = client.DoNoRedirectStream(t.Context(), http.MethodPost, server.URL+"/api", apiHeaders, nil)
				if errRequest != nil {
					t.Fatal(errRequest)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				headers = <-observed
				apiHeaders["User-Agent"] = client.persona.UserAgent
				apiHeaders["Accept-Language"] = client.persona.AcceptLanguage
				apiHeaders["DNT"] = "1"
				for key, value := range apiHeaders {
					if values := headers.Values(key); !reflect.DeepEqual(values, []string{value}) {
						t.Errorf("API %s = %q, want only %q", key, values, value)
					}
				}
				for _, key := range []string{"Sec-Fetch-User", "Upgrade-Insecure-Requests"} {
					if values := headers.Values(key); len(values) != 0 {
						t.Errorf("API inherited document header %s = %q", key, values)
					}
				}
			}
		})
	}
}
