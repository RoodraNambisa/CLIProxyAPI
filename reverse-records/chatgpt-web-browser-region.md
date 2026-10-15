# ChatGPT Web browser region

## Configuration

This feature affects ChatGPT Web only. Both automatic switches default to off.
The request environment is separate from persistent Persona, device identity,
cookies and browser-environment identifiers.

```yaml
chatgpt-web:
  timezone: UTC
  # timezone-offset-minutes: 0
  browser-region:
    auto-timezone: false
    auto-language: false
    language: ""
    lookup-url: "https://ipwho.is/{ip}"
    lookup-timeout-seconds: 8
    cache-ttl-seconds: 21600
    route-cache-size: 4096
    ip-cache-size: 4096
```

- Automatic timezone uses the returned IANA zone and computes its current DST
  offset when a request starts. It does not inherit a manual offset override.
- Automatic language selects a regional default through `x/text/language`, not
  a detected user preference. An empty manual language uses the credential.
- Metadata requests follow the already resolved route. They have no account
  authorization, cookies or device identity and never change proxy bindings.
- A URL containing `{ip}` uses an official anonymous trace to observe the exit,
  then queries that exact IP. Both requests use the selected route. A URL without
  the placeholder uses the lookup service's observed exit, explicitly labeled
  as such. Rotating and destination-dependent proxies can produce different exits.
- The lookup service must support HTTPS and return `success`, `ip`, `country_code`,
  optional `region_code`, and `timezone.id`. Responses are limited to 64 KiB.
  Redirects are not followed. Returned query IPs and IANA zones are validated.

The standard service documents its response contract and rate limiting at
[ipwhois.io/documentation](https://ipwhois.io/documentation). A compatible service
can be configured without changing provider execution behavior.

## Lazy refresh contract

There is no scheduled full scan. Business requests read memory and enqueue at
most one refresh per route without waiting. Provider-owned work is limited to
two workers and 256 waiting jobs; workers start only when needed.

The route TTL is a soft refresh interval with 20 percent jitter. Successful IP
metadata is fresh for at most 24 hours. Expiry does not delete successful values:
applicable stale values serve requests while a refresh runs or fails. If no
applicable successful value exists, manual settings and credential/default
values apply. A newly observed IP invalidates the previous exit's region.

Failures use lazy exponential retry cooldown, starting at approximately 30
seconds and capped at 30 minutes. Valid `Retry-After` values may extend it. The
end of cooldown does not itself start work. Known identical IP lookups coalesce.

Route keys hash the selected proxy identity, not the account identity. Raw proxy
authentication stays private. Both LRU capacities are configurable positive
integers; large independent active route sets may require larger caches.
Shrinking evicts least-recently-used entries. Changing the TTL recomputes refresh
eligibility without issuing requests. Changing the lookup service invalidates
its results. Generation checks prevent canceled, old work from publishing.
Shutdown cancels probes and releases pending jobs and caches. Nothing is persisted.

One logical execution or login flow pins its environment. Header language,
request timezone, proof data, Go VM and SDK receive the same snapshot. Temporary
values do not enter credential persistence. Remote Sentinel responses declare
`browser-region-v1`; unsupported nodes cannot silently discard locale data and
follow the configured local fallback policy or return an incompatibility error.

## Management

All paths are relative to the existing authenticated management prefix:

| Method and path | Behavior |
| --- | --- |
| `GET /chatgpt-web/browser-region` | Effective settings, including manual timezone/offset |
| `PATCH /chatgpt-web/browser-region` | Supplied fields only; validate, persist atomically, hot reload |
| `GET /chatgpt-web/browser-region/preview?auth_index=INDEX` | Read-only route/cache preview; no network or binding allocation |
| `POST /chatgpt-web/browser-region/refresh` | Body `{"auth_index":"INDEX"}`; enqueue/coalesce and return 202 |

PATCH uses the browser-region keys above plus `timezone` and
`timezone-offset-minutes`. Setting the offset to `null` restores DST derivation.
Queue saturation/cooldown returns 429 with `Retry-After`; an unresolved route
returns 409 on refresh. Startup initialization/failure blocks mutations.

The ChatGPT Web configuration panel exposes these fields, paginated credential
search and saved-configuration previews. It displays fresh/stale/fallback sources,
age and safe error reasons. Preview reads do not apply unsaved drafts. Polling
is bounded and can be resumed explicitly. Connection-scoped requests and aborts
prevent late responses from overwriting another server's state. Unsupported
backends are shown as unavailable. Other provider panels are unchanged.

## Acceptance, 2026-10-16 (Asia/Tokyo)

Each real test used one authorized credential copy in memory, fresh request IDs
and the ordinary executor. No browser-prepared challenge was replayed. Automatic
timezone and language were enabled, image request/bootstrap budgets were zero,
and SDK fallback counts were zero. A cold request snapshot remained UTC; the
subsequent image execution used the warmed automatic region snapshot.

| Isolated route | Effective timezone / JS offset | Language | Image elapsed (excluding initial lookup) | PNG |
| --- | --- | --- | --- | --- |
| Supplied US proxy | America/Los_Angeles / +420 | en-US | 25,259 ms | 1448 × 1086 |
| Supplied JP proxy | Asia/Tokyo / −540 | ja-JP | 49,699 ms | 1448 × 1086 |
| OVH2 native IPv4 | America/Toronto / +240 | en-CA | 30,221 ms | 1448 × 1086 |
| OVH2 native IPv6 | America/Toronto / +240 | en-CA | 46,731 ms | 1374 × 1145 |

The native-family controls used a loopback-only CONNECT forwarder that preserved
end-to-end TLS and forced every dial to the selected family. Each reported five
connections, zero wrong-family connections and zero dial errors. No production
listener, configuration, stored credential, password or MFA setting changed.

The tested Linux binary was built from production sources through `93151aba5`
with an overlay adding only the acceptance entrypoint. SHA-256:
`e2a6576479f2db02d10343ea0d37133bbf844f27a25f7ff3bcd0d219a07e624d`.
Subsequent review changes cover TTL hot-update eligibility, shutdown release and
rejecting a second locale pin after a fallback was frozen. They do not change the
tested single-pin wire values. These samples do not guarantee every proxy/account.

### Reproduction

The committed opt-in `TestChatGPTWebBrowserRegionLive` is skipped in ordinary CI.
Supply a private JSON file containing `metadata`, `probe_proxy` and optional
`images`, using a separately authorized test credential. Never commit that file.
Set `CPA_BROWSER_REGION_LIVE=1`, `CPA_VERIFY_INPUT`, `CPA_VERIFY_RESULT`,
`CPA_VERIFY_IMAGE` and `CPA_VERIFY_EXIT_HASH` (the first 16 hex characters of the
SHA-256 of the expected trace IP). Optional `CPA_VERIFY_NETWORK=tcp4|tcp6` selects
a native-family control; `CPA_VERIFY_METADATA_ONLY=1` skips image submission.

```sh
go test ./internal/runtime/executor -run '^TestChatGPTWebBrowserRegionLive$' -count=1 -timeout=210s
```

The harness does not print account material. On quota, lookup or external
failure, retain the safe result as a failed/incomplete trial rather than a pass.

## Reviews and verification

Per-block reviews covered transport cancellation, provider scope, cache bounds,
credential isolation, strict management validation and frontend save coordination.
Overall review one added regressions for hot TTL changes, shutdown release,
late-generation results and preview-versus-capability errors. Overall review two
cross-checked source consumers, wire/locale compatibility, staging, cancellation
and the real-test evidence boundary; it also locked down the empty-locale fallback
against a second, same-timezone pin.

Validation includes focused repeated tests, relevant package Race, full backend
tests, Vet, server build and diff checks; frontend full tests, Lint, type checking,
production build and source/visual roundtrips. The final frontend suite passed
209 files and 1509 tests. Desktop and 390-pixel fixture layouts were visually
checked. All delivery is local; no push or deployment.
