package chatgptweb

const (
	sentinelBrowserFingerprintVersion = personaCatalogVersion
	sentinelBrowserFingerprintSlots   = 8
)

type sentinelBrowserPlatform uint8

const (
	sentinelBrowserPlatformUnknown sentinelBrowserPlatform = iota
	sentinelBrowserPlatformMac
	sentinelBrowserPlatformWindows
	sentinelBrowserPlatformLinux
)

type sentinelBrowserProfile struct {
	version          string
	catalogID        string
	slot             int
	platform         sentinelBrowserPlatform
	webGLVendor      string
	webGLRenderer    string
	availLeft        int
	availTop         int
	availWidth       int
	availHeight      int
	innerWidth       int
	innerHeight      int
	devicePixelRatio float64
	deviceMemory     float64
	jsHeapSizeLimit  float64
	colorDepth       int
	maxTouchPoints   int
}

func resolveSentinelBrowserProfile(environment ConversationTurnstileEnvironment) sentinelBrowserProfile {
	entry, index := personaCatalogRuntimeEntry(environment.Persona)
	return sentinelBrowserProfileForCatalogEntry(entry, index)
}

// sentinelBrowserProfileForSlot is retained for deterministic browser VM tests.
// Production requests resolve the slot from the credential's catalog ID.
func sentinelBrowserProfileForSlot(_ Persona, slot int) sentinelBrowserProfile {
	slot %= len(personaCatalogV2)
	if slot < 0 {
		slot += len(personaCatalogV2)
	}
	return sentinelBrowserProfileForCatalogEntry(personaCatalogV2[slot], slot)
}

func sentinelBrowserProfileForCatalogEntry(entry personaCatalogEntry, index int) sentinelBrowserProfile {
	return sentinelBrowserProfile{
		version:          sentinelBrowserFingerprintVersion,
		catalogID:        entry.persona.CatalogID,
		slot:             index,
		platform:         entry.platform,
		webGLVendor:      entry.webGLVendor,
		webGLRenderer:    entry.webGLRenderer,
		availLeft:        entry.availLeft,
		availTop:         entry.availTop,
		availWidth:       entry.availWidth,
		availHeight:      entry.availHeight,
		innerWidth:       entry.innerWidth,
		innerHeight:      entry.innerHeight,
		devicePixelRatio: entry.devicePixelRatio,
		deviceMemory:     entry.deviceMemory,
		jsHeapSizeLimit:  entry.jsHeapSizeLimit,
		colorDepth:       entry.colorDepth,
		maxTouchPoints:   entry.maxTouchPoints,
	}
}
