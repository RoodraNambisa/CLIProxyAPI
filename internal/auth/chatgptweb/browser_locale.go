package chatgptweb

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/text/language"
)

type browserRegionFlowKey struct{}
type browserRegionFlow struct {
	mu       sync.Mutex
	snapshot *BrowserRegionSnapshot
}

func withBrowserRegionFlow(ctx context.Context) context.Context {
	return context.WithValue(ctx, browserRegionFlowKey{}, &browserRegionFlow{})
}
func (service *Service) prepareBrowserRegionClient(ctx context.Context, client *Client) error {
	if service.options.ResolveBrowserRegion == nil {
		return nil
	}
	if flow, _ := ctx.Value(browserRegionFlowKey{}).(*browserRegionFlow); flow != nil {
		flow.mu.Lock()
		defer flow.mu.Unlock()
		if flow.snapshot == nil {
			value := service.options.ResolveBrowserRegion(ctx, client.ProxyURL(), client.Persona())
			flow.snapshot = &value
		}
		return client.SetBrowserRegion(*flow.snapshot)
	}
	return client.SetBrowserRegion(service.options.ResolveBrowserRegion(ctx, client.ProxyURL(), client.Persona()))
}

// BrowserLocale is a transient overlay, separate from the canonical device Persona.
type BrowserLocale struct {
	Language string `json:"language"`
}

func (locale BrowserLocale) normalized() (BrowserLocale, error) {
	tag, err := language.Parse(strings.TrimSpace(locale.Language))
	if err != nil || tag == language.Und || len(locale.Language) > 128 {
		return BrowserLocale{}, fmt.Errorf("invalid browser language")
	}
	return BrowserLocale{Language: tag.String()}, nil
}
func (locale BrowserLocale) Languages() []string {
	tag := language.Make(locale.Language)
	base, _ := tag.Base()
	if tag.String() == base.String() {
		return []string{tag.String()}
	}
	return []string{tag.String(), base.String()}
}
func (locale BrowserLocale) AcceptLanguage() string {
	values := locale.Languages()
	if len(values) == 1 {
		return values[0]
	}
	return values[0] + "," + values[1] + ";q=0.9"
}
func (client *Client) SetBrowserRegion(snapshot BrowserRegionSnapshot) error {
	// An empty locale pins the credential fallback without changing its encoding.
	locale := &BrowserLocale{}
	if snapshot.OverrideLanguage {
		value, err := (BrowserLocale{Language: snapshot.Language}).normalized()
		if err != nil {
			return err
		}
		locale = &value
	}
	if err := client.SetBrowserTimezone(BrowserTimezone{Name: snapshot.Timezone, OffsetMinutes: snapshot.OffsetMinutes}); err != nil {
		return err
	}
	if !client.browserLocale.CompareAndSwap(nil, locale) {
		if previous := client.browserLocale.Load(); previous == nil || *previous != *locale {
			return fmt.Errorf("browser locale is already pinned")
		}
	}
	return nil
}
func (client *Client) BrowserLocale() *BrowserLocale {
	if client != nil {
		if locale := client.browserLocale.Load(); locale != nil && locale.Language != "" {
			copy := *locale
			return &copy
		}
	}
	return nil
}
func (environment ConversationTurnstileEnvironment) browserLanguage() (string, []string) {
	if environment.Locale != nil {
		return environment.Locale.Language, environment.Locale.Languages()
	}
	persona := canonicalPersona(environment.Persona)
	return persona.Language, personaNavigatorLanguages(persona)
}
