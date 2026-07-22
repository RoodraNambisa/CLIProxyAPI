# fastschema/qjs

The Sentinel fallback runtime uses `github.com/fastschema/qjs` version `v0.0.6`, corresponding to commit `461716f4f380f81ffd09378751f1812919cddbca`.

The dependency embeds QuickJS-NG from commit `d01ca4491fb24ccfeccb4c7394e28a3b21fd5986` (runtime version `0.10.1`) in `qjs.wasm`. The embedded file in qjs `v0.0.6` has SHA-256 `88fcbeead33cca3ec8394feb0fb3eec8047ec9ac5062aba7a63799cee11989b7`.

qjs uses `github.com/tetratelabs/wazero` version `v1.9.0`, commit `96f2052f6d12cccc29193f5452c635b76d8a036d`, to execute the WebAssembly module. CLIProxyAPI adds its own fixed worker limit, priority queue, per-request runtime isolation, cache policy, and lifecycle management instead of using the dependency's general-purpose pool.

Included notices:

- `LICENSE`: fastschema/qjs MIT license.
- `LICENSE.quickjs-ng`: embedded QuickJS-NG MIT license.
- `LICENSE.wazero`: wazero Apache License 2.0.
- `NOTICE.wazero`: wazero attribution notice.
