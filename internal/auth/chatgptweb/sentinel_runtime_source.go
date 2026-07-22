package chatgptweb

import _ "embed"

// sentinelSDKRuntimeSource is this package's isolated browser environment and
// bridge for a fetched Sentinel SDK.
//
//go:embed sentinel_sdk_runtime.js
var sentinelSDKRuntimeSource string
