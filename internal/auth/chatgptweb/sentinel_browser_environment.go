package chatgptweb

import (
	"crypto/sha256"
	"strconv"
	"strings"
)

const (
	sentinelBrowserFingerprintVersion = "v1"
	sentinelBrowserFingerprintSlots   = 1 << 12
)

type sentinelBrowserPlatform uint8

const (
	sentinelBrowserPlatformUnknown sentinelBrowserPlatform = iota
	sentinelBrowserPlatformMac
	sentinelBrowserPlatformWindows
	sentinelBrowserPlatformLinux
)

type sentinelBrowserRenderProfile struct {
	vendor   string
	renderer string
}

type sentinelBrowserWorkspaceProfile struct {
	availLeft        int
	availTop         int
	availWidth       int
	availHeight      int
	innerWidth       int
	innerHeight      int
	devicePixelRatio float64
}

type sentinelBrowserCapacityProfile struct {
	deviceMemory    float64
	jsHeapSizeLimit float64
	colorDepth      int
}

type sentinelBrowserProfile struct {
	version          string
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

var sentinelMacRenderProfiles = [...]sentinelBrowserRenderProfile{
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M1, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M1 Pro, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M1 Max, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M2, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M2 Pro, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M2 Max, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M3, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M3 Pro, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M3 Max, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M4, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M4 Pro, Unspecified Version)"},
	{vendor: "Google Inc. (Apple)", renderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M4 Max, Unspecified Version)"},
	{vendor: "Google Inc. (Intel Inc.)", renderer: "ANGLE (Intel Inc., Intel(R) Iris(TM) Plus Graphics 645, OpenGL 4.1)"},
	{vendor: "Google Inc. (Intel Inc.)", renderer: "ANGLE (Intel Inc., Intel(R) UHD Graphics 630, OpenGL 4.1)"},
	{vendor: "Google Inc. (ATI Technologies Inc.)", renderer: "ANGLE (ATI Technologies Inc., AMD Radeon Pro 5500M OpenGL Engine, OpenGL 4.1)"},
	{vendor: "Google Inc. (ATI Technologies Inc.)", renderer: "ANGLE (ATI Technologies Inc., AMD Radeon Pro 5600M OpenGL Engine, OpenGL 4.1)"},
}

var sentinelWindowsRenderProfiles = [...]sentinelBrowserRenderProfile{
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 Ti Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 2060 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 3070 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 3080 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 4060 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 4070 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 4080 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (Intel)", renderer: "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (Intel)", renderer: "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (Intel)", renderer: "ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 580 Series Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 6600 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 6700 XT Direct3D11 vs_5_0 ps_5_0, D3D11)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 7800 XT Direct3D11 vs_5_0 ps_5_0, D3D11)"},
}

var sentinelLinuxRenderProfiles = [...]sentinelBrowserRenderProfile{
	{vendor: "Google Inc. (Intel)", renderer: "ANGLE (Intel, Mesa Intel(R) UHD Graphics 620 (KBL GT2), OpenGL 4.6)"},
	{vendor: "Google Inc. (Intel)", renderer: "ANGLE (Intel, Mesa Intel(R) UHD Graphics 630 (CFL GT2), OpenGL 4.6)"},
	{vendor: "Google Inc. (Intel)", renderer: "ANGLE (Intel, Mesa Intel(R) Iris(R) Xe Graphics (TGL GT2), OpenGL 4.6)"},
	{vendor: "Google Inc. (Intel)", renderer: "ANGLE (Intel, Mesa Intel(R) Arc(TM) A380 Graphics, OpenGL 4.6)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 560 Series (radeonsi), OpenGL 4.6)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 580 Series (radeonsi), OpenGL 4.6)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 6600 (radeonsi), OpenGL 4.6)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 6700 XT (radeonsi), OpenGL 4.6)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 6800 XT (radeonsi), OpenGL 4.6)"},
	{vendor: "Google Inc. (AMD)", renderer: "ANGLE (AMD, AMD Radeon RX 7600 (radeonsi), OpenGL 4.6)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 Ti, OpenGL 4.6)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 2060, OpenGL 4.6)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060, OpenGL 4.6)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 3070, OpenGL 4.6)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 4060, OpenGL 4.6)"},
	{vendor: "Google Inc. (NVIDIA)", renderer: "ANGLE (NVIDIA, NVIDIA GeForce RTX 4070, OpenGL 4.6)"},
}

var sentinelCapacityProfiles = [...]sentinelBrowserCapacityProfile{
	{deviceMemory: 4, jsHeapSizeLimit: 2_147_352_576, colorDepth: 24},
	{deviceMemory: 4, jsHeapSizeLimit: 2_199_023_255, colorDepth: 24},
	{deviceMemory: 4, jsHeapSizeLimit: 4_244_373_504, colorDepth: 24},
	{deviceMemory: 4, jsHeapSizeLimit: 4_294_705_152, colorDepth: 24},
	{deviceMemory: 8, jsHeapSizeLimit: 4_294_967_296, colorDepth: 24},
	{deviceMemory: 8, jsHeapSizeLimit: 4_345_298_944, colorDepth: 24},
	{deviceMemory: 8, jsHeapSizeLimit: 4_395_630_592, colorDepth: 24},
	{deviceMemory: 8, jsHeapSizeLimit: 4_445_962_240, colorDepth: 24},
	{deviceMemory: 8, jsHeapSizeLimit: 4_294_967_296, colorDepth: 30},
	{deviceMemory: 8, jsHeapSizeLimit: 4_345_298_944, colorDepth: 30},
	{deviceMemory: 8, jsHeapSizeLimit: 4_395_630_592, colorDepth: 30},
	{deviceMemory: 8, jsHeapSizeLimit: 4_445_962_240, colorDepth: 30},
	{deviceMemory: 8, jsHeapSizeLimit: 4_496_293_888, colorDepth: 24},
	{deviceMemory: 8, jsHeapSizeLimit: 4_546_625_536, colorDepth: 24},
	{deviceMemory: 8, jsHeapSizeLimit: 4_596_957_184, colorDepth: 30},
	{deviceMemory: 8, jsHeapSizeLimit: 4_647_288_832, colorDepth: 30},
}

func resolveSentinelBrowserProfile(environment ConversationTurnstileEnvironment) sentinelBrowserProfile {
	persona := normalizePersona(environment.Persona)
	deviceID := strings.TrimSpace(environment.DeviceID)
	if deviceID == "" {
		deviceID = persona.Profile + "\x00" + persona.UserAgent + "\x00" + persona.Platform
	}
	digest := sha256.Sum256([]byte("chatgpt-web-sentinel-fingerprint-" + sentinelBrowserFingerprintVersion + "\x00" + deviceID))
	slot := int(digest[0])<<4 | int(digest[1]>>4)
	return sentinelBrowserProfileForSlot(persona, slot)
}

func sentinelBrowserProfileForSlot(persona Persona, slot int) sentinelBrowserProfile {
	persona = normalizePersona(persona)
	slot &= sentinelBrowserFingerprintSlots - 1
	renderIndex := slot & 0x0f
	workspaceIndex := slot >> 4 & 0x0f
	capacityIndex := slot >> 8 & 0x0f
	platform := sentinelBrowserPlatformForPersona(persona)
	renderProfile := sentinelBrowserRenderProfileForPlatform(platform, renderIndex)
	workspaceProfile := sentinelBrowserWorkspaceProfileForSlot(platform, persona.ScreenWidth, persona.ScreenHeight, workspaceIndex)
	capacityProfile := sentinelCapacityProfiles[capacityIndex]
	return sentinelBrowserProfile{
		version:          sentinelBrowserFingerprintVersion,
		slot:             slot,
		platform:         platform,
		webGLVendor:      renderProfile.vendor,
		webGLRenderer:    renderProfile.renderer,
		availLeft:        workspaceProfile.availLeft,
		availTop:         workspaceProfile.availTop,
		availWidth:       workspaceProfile.availWidth,
		availHeight:      workspaceProfile.availHeight,
		innerWidth:       workspaceProfile.innerWidth,
		innerHeight:      workspaceProfile.innerHeight,
		devicePixelRatio: workspaceProfile.devicePixelRatio,
		deviceMemory:     capacityProfile.deviceMemory,
		jsHeapSizeLimit:  capacityProfile.jsHeapSizeLimit,
		colorDepth:       capacityProfile.colorDepth,
		maxTouchPoints:   0,
	}
}

func sentinelBrowserPlatformForPersona(persona Persona) sentinelBrowserPlatform {
	value := strings.ToLower(strings.TrimSpace(persona.Platform + " " + persona.UserAgent))
	switch {
	case strings.Contains(value, "mac"):
		return sentinelBrowserPlatformMac
	case strings.Contains(value, "win"):
		return sentinelBrowserPlatformWindows
	case strings.Contains(value, "linux") || strings.Contains(value, "x11"):
		return sentinelBrowserPlatformLinux
	default:
		return sentinelBrowserPlatformUnknown
	}
}

func sentinelBrowserRenderProfileForPlatform(platform sentinelBrowserPlatform, index int) sentinelBrowserRenderProfile {
	index &= 0x0f
	switch platform {
	case sentinelBrowserPlatformMac:
		return sentinelMacRenderProfiles[index]
	case sentinelBrowserPlatformWindows:
		return sentinelWindowsRenderProfiles[index]
	case sentinelBrowserPlatformLinux:
		return sentinelLinuxRenderProfiles[index]
	default:
		profile := sentinelLinuxRenderProfiles[index]
		profile.vendor = "Google Inc."
		profile.renderer = "ANGLE (Google, Vulkan 1.3 Renderer " + strconv.Itoa(index+1) + ", Vulkan)"
		return profile
	}
}

func sentinelBrowserWorkspaceProfileForSlot(platform sentinelBrowserPlatform, width, height, index int) sentinelBrowserWorkspaceProfile {
	index &= 0x0f
	if width <= 0 {
		width = DefaultPersona().ScreenWidth
	}
	if height <= 0 {
		height = DefaultPersona().ScreenHeight
	}
	scale := index & 0x03
	layout := index >> 2
	dprValues := [4]float64{1, 1.25, 1.5, 2}
	topValues := [4]int{0, 0, 0, 0}
	bottomValues := [4]int{0, 40, 48, 64}
	leftValues := [4]int{0, 0, 0, 64}
	if platform == sentinelBrowserPlatformMac {
		topValues = [4]int{0, 24, 25, 24}
		bottomValues = [4]int{0, 48, 80, 0}
		leftValues = [4]int{0, 0, 0, 72}
	} else if platform == sentinelBrowserPlatformLinux {
		bottomValues = [4]int{0, 24, 32, 48}
		leftValues = [4]int{0, 0, 0, 48}
	}
	left := min(leftValues[layout], max(0, width-1))
	top := min(topValues[layout], max(0, height-1))
	availableWidth := max(1, width-left)
	availableHeight := max(1, height-top-min(bottomValues[layout], max(0, height-top-1)))
	innerWidth := max(1, availableWidth-scale*16)
	innerHeight := max(1, availableHeight-72-scale*8)
	return sentinelBrowserWorkspaceProfile{
		availLeft:        left,
		availTop:         top,
		availWidth:       availableWidth,
		availHeight:      availableHeight,
		innerWidth:       innerWidth,
		innerHeight:      innerHeight,
		devicePixelRatio: dprValues[scale],
	}
}
