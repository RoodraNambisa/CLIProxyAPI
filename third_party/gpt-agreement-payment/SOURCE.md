# ChatGPT Authentication Reference

The password and TOTP challenge flow was validated against a local reference checkout whose configured public remotes are:

- `RoodraNambisa/Gpt-Agreement-Payment`, remote baseline `1b8c1582551b3578aa94bfd1ffd4a6663fae1c51`.
- `zero199901/gpt-pp-team`, remote baseline `a45fd449fa5efe9708bce657decf891e9ed08643`.

The exact local reference snapshot is `db7236f1b3025fd7d65136d67b36234b9116400b`. This commit and the earlier local authentication snapshot `7a004ff3879f53b0461401b4b3fcd727676ff5b0` are not published on either configured remote. The Go implementation follows the observed `mfa_challenge` selection, RFC 6238 code generation, `mfa_request_id` propagation, and `/api/accounts/mfa/verify` request shape. The accompanying MIT `LICENSE` is retained from that local snapshot.

Sentinel challenge and Session Observer behavior was also validated against the same local snapshot. Sentinel support first appeared in local commit `922b4f3d9007c16b75384c1f89623ed456904640`, which is not published on either configured remote:

- `src/gpt_agreement_payment/automation/registration/sentinel/quickjs.py`, SHA-256 `588d9473dcad489cfabef3759d42b9fa26e7307a1e2a7d93e3fe2b60b14ed3ab`.
- `src/gpt_agreement_payment/automation/registration/sentinel/assets/openai_sentinel_quickjs.js`, SHA-256 `dd0d1fa79500c502e7ae2f65dac0a9674a97d8c69c27158adfccfbdc434a90bd`.
- Real SDK fixture `sentinel-sdk-20260219f9f6.js`, SHA-256 `4f8ef8d5870894fd0101fc40ff45ea13c0f8e25c71c2ba28e5df5baf98babbb5`.

The unpublished local snapshot is development provenance, not a build or runtime dependency, and its Sentinel source is not redistributed here. The Sentinel browser environment, SDK adapter, scheduler, and cache implementation in this repository were independently authored for the in-process, no-CGO QuickJS/WASM runtime. The reference was used only to validate protocol behavior.

At runtime, dynamic SDK updates are trusted only from the official OpenAI HTTPS hosts and exact versioned Sentinel SDK paths (`/sentinel/<version>/sdk.js`, plus ChatGPT's `/backend-api/sentinel/<version>/sdk.js`). Redirect targets are validated again, bootstrap SHA-256 subresource integrity is enforced when supplied, and the downloaded digest is always used as the cache, compatibility, and circuit-breaker identity. Remote SDK source, compiled bytecode, compatibility decisions, and Observer state remain memory-only.

The validated fixture hash selects its pinned adapter, but still has to satisfy that adapter's source-shape contract. A future SDK hash is accepted only when its source uniquely matches the same export marker and every required minified symbol declaration; the selected adapter is then bound to that exact source hash for cache identity. A changed or ambiguous source shape fails closed instead of guessing renamed symbols.
