# Sentinel computation service

The default is local computation. Remote computation is opt-in and independent
of image generation, polling, downloads, credentials and official HTTP requests.
Never expose the computation listener without authentication. Use HTTPS across
public networks; cleartext HTTP is accepted only for explicit private IPs and
localhost. Keep management keys separate from solver keys.

## Dedicated node

Start a second CPA process with its own local configuration:

```yaml
host: 127.0.0.1
port: 8317
remote-management:
  secret-key: "replace-with-management-secret"
sentinel-solver:
  enabled: true
  listen: "127.0.0.1:8318"
  api-keys: ["replace-with-solver-secret"]
  sdk-fallback-enabled: false
  memory-budget-mib: 512
```

```sh
./cli-proxy-api --sentinel-solver-only --config /path/to/solver.yaml
```

The management page is `/management.html` on the management listener. Dedicated
mode skips PG/git/object token stores, auth scanning, login/refresh workers,
model updates and proxy routes, even if storage environment variables exist.
Normal proxy instances may enable the same independent computation listener.
Avoid reusing the same configuration/ports for two local instances.

## Caller

```yaml
chatgpt-web:
  sentinel:
    mode: remote
    sdk-runtime-enabled: true
    remote:
      scopes: [images]
      budget-seconds: 30
      nodes:
        - name: primary
          url: "https://solver.example.com"
          api-key: "replace-with-solver-secret"
        - name: secondary
          url: "https://solver-2.example.com"
          api-key: "replace-with-other-solver-secret"
```

Supported scopes are `images`, `chat`, and `login`. Omitting scopes selects only
images; an explicit empty list selects none. Ordinary chat with an input image
is not image generation. The RPC budget is cumulative per challenge round,
including retries, and excludes official HTTP waiting. A shorter parent request
budget always wins. SDK fallback is never attempted after parent cancellation.

New sessions round-robin across available nodes. All stages within one session
stay on the same node/process; do not put a non-sticky load balancer behind one
configured address. A remote failure may use the caller's local fallback,
retaining the original requirements token and challenge. It does not submit a
second image request or cool down an account. Existing local Go implementations
remain the fallback for requirements/proof; the SDK handles Turnstile/Observer.

## Updates and limits

The node owns immutable compatibility rule generations. New sessions see new
rules, while existing sessions and local SDK reconstruction retain their pinned
rules. Listener/TLS changes require restarting that instance. Revoke API keys
only after callers have received the replacement key.

For rolling updates, run at least two separate solver processes, stop routing
new sessions to the target node, and let it drain before replacing its binary.
Active sessions may fall back locally if a node disappears. Updating a solver
cohosted in the proxy process still requires restarting that proxy process.
An incompatible wire protocol or changed official HTTP choreography may still
require a caller upgrade.

Memory admission reserves 128 MiB per retained session and 96 MiB per active RPC
for bounded VM/input/result/SDK and JSON buffer headroom. Creation reserves both
atomically to avoid exhausting intake capacity before a session can start.
The configured budget must be at least 256 MiB. `max-sessions` is also enforced, but
does not override the memory budget. This reservation is not a hard RSS cap;
leave room for the Go runtime and enforce a container/cgroup memory limit when
needed. Every program also retains its existing steps, values and heap limits.
Use the authenticated health endpoint and management counters to tune capacity.

RPC bodies contain sensitive challenge material. They are never stored in
normal proxy request logs. Solver nodes do not need account tokens, passwords,
cookies, prompts or images. SDK code is fetched only from validated official
HTTPS resources with per-session integrity checks. The v1 API consists of
health, session creation, solve, snapshot, keepalive and idempotent close.
