package chatgptweb

const (
	sentinelBrowserFingerprintVersion = browserEnvironmentCatalogVersion
	sentinelBrowserFingerprintSlots   = browserEnvironmentVariantsPerPersona
)

type sentinelBrowserPlatform uint8

const (
	sentinelBrowserPlatformUnknown sentinelBrowserPlatform = iota
	sentinelBrowserPlatformMac
	sentinelBrowserPlatformWindows
	sentinelBrowserPlatformLinux
)

type sentinelBrowserProfile struct {
	version             string
	catalogID           string
	slot                int
	platform            sentinelBrowserPlatform
	webGLVendor         string
	webGLRenderer       string
	screenWidth         int
	screenHeight        int
	hardwareConcurrency int
	availLeft           int
	availTop            int
	availWidth          int
	availHeight         int
	innerWidth          int
	innerHeight         int
	outerWidth          int
	outerHeight         int
	devicePixelRatio    float64
	deviceMemory        float64
	jsHeapSizeLimit     float64
	colorDepth          int
	maxTouchPoints      int
}

type sentinelBrowserWindowLayout struct {
	widthPercent  int
	heightPercent int
}

// sentinelBrowserWindowLayouts is immutable. These layouts vary only the
// browser viewport within a fixed hardware family and never change its screen,
// DPR, GPU, platform, or transport identity.
var sentinelBrowserWindowLayouts = [...]sentinelBrowserWindowLayout{
	{widthPercent: 100, heightPercent: 100},
	{widthPercent: 92, heightPercent: 96},
	{widthPercent: 84, heightPercent: 92},
	{widthPercent: 76, heightPercent: 88},
	{widthPercent: 68, heightPercent: 84},
	{widthPercent: 88, heightPercent: 80},
	{widthPercent: 80, heightPercent: 72},
	{widthPercent: 72, heightPercent: 64},
}

// sentinelBrowserExtendedWindowLayouts adds viewport shapes without changing
// the meaning of the persisted e00-e31 catalog identities.
var sentinelBrowserExtendedWindowLayouts = [...]sentinelBrowserWindowLayout{
	{widthPercent: 96, heightPercent: 92},
	{widthPercent: 90, heightPercent: 88},
	{widthPercent: 86, heightPercent: 84},
	{widthPercent: 82, heightPercent: 80},
	{widthPercent: 78, heightPercent: 76},
	{widthPercent: 74, heightPercent: 72},
	{widthPercent: 70, heightPercent: 68},
	{widthPercent: 66, heightPercent: 60},
}

// The additional layout dimensions extend v3 from e64 onward. They deliberately
// vary only ordinary window sizes within the selected hardware family; the
// Chrome version, TLS profile, UA, platform, screen, DPR, GPU, CPU, memory and
// language remain unchanged. Keep both arrays ordered and immutable because
// their Cartesian-product order is part of the persisted catalog identity.
var sentinelBrowserAdditionalWindowWidths = [...]int{
	99, 97, 95, 93, 91, 89, 87, 85, 83, 81, 79, 77,
}

var sentinelBrowserAdditionalWindowHeights = [...]int{
	98, 94, 90, 86,
}

// These four Chrome heap limits come from the previously verified capacity
// table. Device memory and color depth remain fixed by the hardware family.
var sentinelBrowserHeapProfiles = [...]float64{
	4_294_967_296,
	4_345_298_944,
	4_395_630_592,
	4_445_962_240,
}

func resolveSentinelBrowserProfile(environment ConversationTurnstileEnvironment) sentinelBrowserProfile {
	entry, _ := personaCatalogRuntimeEntry(environment.Persona)
	identity := environment.BrowserEnvironment
	slot, ok := browserEnvironmentSlot(entry.persona, identity)
	if !ok {
		identity = browserEnvironmentIdentityForSeed(entry.persona, environment.DeviceID)
		slot, ok = browserEnvironmentSlot(entry.persona, identity)
	}
	if !ok {
		slot = 0
	}
	return sentinelBrowserProfileForCatalogEntry(entry, slot)
}

// sentinelBrowserProfileForSlot is retained for deterministic browser VM tests.
// Production requests carry the credential's persisted browser environment ID.
func sentinelBrowserProfileForSlot(persona Persona, slot int) sentinelBrowserProfile {
	entry, _ := personaCatalogRuntimeEntry(persona)
	return sentinelBrowserProfileForCatalogEntry(entry, slot)
}

func sentinelBrowserProfileForCatalogEntry(entry personaCatalogEntry, slot int) sentinelBrowserProfile {
	variantCount := browserEnvironmentVariantCount(entry.persona)
	slot %= variantCount
	if slot < 0 {
		slot += variantCount
	}
	layout, heap := sentinelBrowserLayoutAndHeapForSlot(slot)
	innerWidth := scaleBrowserViewport(entry.innerWidth, layout.widthPercent, 640, entry.availWidth)
	innerHeight := scaleBrowserViewport(entry.innerHeight, layout.heightPercent, 480, entry.availHeight)
	outerWidth := innerWidth
	outerHeight := innerHeight + (entry.availHeight - entry.innerHeight)
	if outerHeight > entry.availHeight {
		outerHeight = entry.availHeight
	}
	identity := browserEnvironmentIdentityForSlot(entry.persona, slot)
	return sentinelBrowserProfile{
		version:             identity.CatalogVersion,
		catalogID:           identity.CatalogID,
		slot:                slot,
		platform:            entry.platform,
		webGLVendor:         entry.webGLVendor,
		webGLRenderer:       entry.webGLRenderer,
		screenWidth:         entry.persona.ScreenWidth,
		screenHeight:        entry.persona.ScreenHeight,
		hardwareConcurrency: entry.persona.HardwareConcurrency,
		availLeft:           entry.availLeft,
		availTop:            entry.availTop,
		availWidth:          entry.availWidth,
		availHeight:         entry.availHeight,
		innerWidth:          innerWidth,
		innerHeight:         innerHeight,
		outerWidth:          outerWidth,
		outerHeight:         outerHeight,
		devicePixelRatio:    entry.devicePixelRatio,
		deviceMemory:        entry.deviceMemory,
		jsHeapSizeLimit:     heap,
		colorDepth:          entry.colorDepth,
		maxTouchPoints:      entry.maxTouchPoints,
	}
}

func sentinelBrowserLayoutAndHeapForSlot(slot int) (sentinelBrowserWindowLayout, float64) {
	legacyBlockSize := len(sentinelBrowserWindowLayouts) * len(sentinelBrowserHeapProfiles)
	extendedBlockSize := len(sentinelBrowserExtendedWindowLayouts) * len(sentinelBrowserHeapProfiles)
	switch {
	case slot < legacyBlockSize:
		return sentinelBrowserWindowLayouts[slot%len(sentinelBrowserWindowLayouts)],
			sentinelBrowserHeapProfiles[slot/len(sentinelBrowserWindowLayouts)]
	case slot < legacyBlockSize+extendedBlockSize:
		index := slot - legacyBlockSize
		return sentinelBrowserExtendedWindowLayouts[index%len(sentinelBrowserExtendedWindowLayouts)],
			sentinelBrowserHeapProfiles[index/len(sentinelBrowserExtendedWindowLayouts)]
	default:
		index := slot - legacyBlockSize - extendedBlockSize
		layoutCount := len(sentinelBrowserAdditionalWindowWidths) * len(sentinelBrowserAdditionalWindowHeights)
		layoutIndex := index % layoutCount
		return sentinelBrowserWindowLayout{
			widthPercent:  sentinelBrowserAdditionalWindowWidths[layoutIndex%len(sentinelBrowserAdditionalWindowWidths)],
			heightPercent: sentinelBrowserAdditionalWindowHeights[layoutIndex/len(sentinelBrowserAdditionalWindowWidths)],
		}, sentinelBrowserHeapProfiles[index/layoutCount]
	}
}

func scaleBrowserViewport(value, percent, minimum, maximum int) int {
	if maximum < 1 {
		maximum = 1
	}
	if minimum > maximum {
		minimum = maximum
	}
	value = value * percent / 100
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}
