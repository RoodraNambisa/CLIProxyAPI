# ChatGPT Web Image Reasoning Modes

`images.chatgpt-web.reasoning-mode` controls the conversation model that invokes
the image tool. It does not select an image engine, change image quality, disable
upstream safety checks, or guarantee that every prompt produces an image.

```yaml
images:
  chatgpt-web:
    upstream-model: auto
    reasoning-mode: instant
```

| Mode | Behavior |
| --- | --- |
| `auto` (default) | Preserve the configured `upstream-model`; do not add a thinking effort. |
| `instant` | Select the cached official Instant category; use the verified `gpt-5-6` carrier when that category is unavailable. Requires `upstream-model: auto` or an empty carrier. |
| `low` | Select an advertised `low` or `min` effort. |
| `medium` | Select an advertised `medium` or `standard` effort. |
| `high` | Select an advertised `high` or `extended` effort. |
| `xhigh` | Select an advertised `extra_high`, `xhigh`, or `max` effort. |

Explicit thinking modes use only capabilities from the selected credential's
cached official model catalog. With an `auto` carrier, the official default
Thinking model is preferred. With a custom carrier, that model must advertise
the requested effort. A missing or unsupported capability produces a local
`chatgpt_web_image_reasoning_unsupported` error before any official request;
existing credential retry limits still apply. The credential is not cooled down
and its fixed-window request allowance is not committed. No silent downgrade or
synchronous model-catalog fetch is performed.

The mode and carrier configuration are pinned for a logical image request,
including credential retries and `n` aggregation. Each credential attempt resolves
its actual model/effort once, and the same pair is used for prepare and submit.
Hot reload affects new logical requests only. Normal chat, image input without
generation, Codex, polling, downloads, and existing moderation classification are
unchanged.

Image prepare and submit both include the website's explicit `picture_v2`
ecosystem mention and `manual_send` metadata. This represents the user's request
to invoke Create Image, instead of relying on a system hint alone. User prompt
content and image attachments are retained.

## Verification Boundaries

The implementation was compared with the website's conversation requests and
model-picker source on 2026-09-25. These are private website protocols and may
change. A Free account may expose Instant and a fixed Thinking Mini model but
no configurable thinking efforts; selecting a higher effort does not grant paid
capabilities. Higher-mode tests use controlled catalog fixtures when a suitable
live account is unavailable.

Capability lookup reads only the selected credential's models under a read lock,
not every credential. A 100,000-credential Instant lookup benchmark is included
in `internal/registry/chatgpt_web_instant_test.go`.

## Homepage Navigation

The homepage bootstrap uses document-navigation headers, not backend API headers.
It carries no Bearer token, `Origin`, `Referer`, `oai-*`, or `x-openai-target-*`
headers. Identity cookies stay in the existing jar. Same-origin redirects keep
this document policy; subsequent backend API calls still carry their normal
authorization and routing headers. TLS fingerprint and transport timeouts are
unchanged.

A controlled homepage-only test on 2026-09-25 kept Chrome 146, egress and the
identity cookie unchanged: the previous API-style headers produced 6/6 HTTP 403s
over IPv4, while document headers produced 6/6 HTTP 200s. IPv6 isolated
connections produced 2/3 HTTP 403s with the old headers and 3/3 HTTP 200s with
document headers. These samples demonstrate a request-shape regression, not a
guarantee against future Cloudflare challenges.
