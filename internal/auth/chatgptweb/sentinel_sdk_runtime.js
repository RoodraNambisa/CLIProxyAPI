(function bootstrapSentinelHost(config, host) {
  "use strict";

  const noop = () => {};
  const asString = (value, fallback = "") => value == null ? fallback : String(value);
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  function invalidBase64Error() {
    const error = new Error("The string to be decoded is not correctly encoded.");
    error.name = "InvalidCharacterError";
    return error;
  }

  function decodeBase64(value) {
    let encoded = asString(value).replace(/[\t\n\f\r ]/g, "");
    if (!/^[A-Za-z0-9+/]*={0,2}$/.test(encoded)) throw invalidBase64Error();
    const paddingPosition = encoded.indexOf("=");
    const remainder = encoded.length % 4;
    if (remainder === 1 || (paddingPosition >= 0 && remainder !== 0)) throw invalidBase64Error();
    if (paddingPosition < 0 && remainder !== 0) encoded += "=".repeat(4 - remainder);
    const bytes = [];
    for (let position = 0; position < encoded.length; position += 4) {
      const a = alphabet.indexOf(encoded[position]);
      const b = alphabet.indexOf(encoded[position + 1]);
      const c = encoded[position + 2] === "=" ? 0 : alphabet.indexOf(encoded[position + 2]);
      const d = encoded[position + 3] === "=" ? 0 : alphabet.indexOf(encoded[position + 3]);
      if (a < 0 || b < 0 || c < 0 || d < 0) throw invalidBase64Error();
      const packed = ((a & 63) << 18) | ((b & 63) << 12) | ((c & 63) << 6) | (d & 63);
      bytes.push((packed >>> 16) & 255);
      if (encoded[position + 2] !== "=") bytes.push((packed >>> 8) & 255);
      if (encoded[position + 3] !== "=") bytes.push(packed & 255);
    }
    return new Uint8Array(bytes);
  }

  function encodeBase64(input) {
    let result = "";
    for (let position = 0; position < input.length; position += 3) {
      const remaining = input.length - position;
      const packed = ((input[position] || 0) << 16) | ((input[position + 1] || 0) << 8) | (input[position + 2] || 0);
      result += alphabet[(packed >>> 18) & 63];
      result += alphabet[(packed >>> 12) & 63];
      result += remaining > 1 ? alphabet[(packed >>> 6) & 63] : "=";
      result += remaining > 2 ? alphabet[packed & 63] : "=";
    }
    return result;
  }

  function utf8Encode(value) {
    const output = [];
    for (const character of asString(value)) {
      const code = character.codePointAt(0);
      if (code < 0x80) {
        output.push(code);
      } else if (code < 0x800) {
        output.push(0xc0 | (code >>> 6), 0x80 | (code & 63));
      } else if (code < 0x10000) {
        output.push(0xe0 | (code >>> 12), 0x80 | ((code >>> 6) & 63), 0x80 | (code & 63));
      } else {
        output.push(0xf0 | (code >>> 18), 0x80 | ((code >>> 12) & 63), 0x80 | ((code >>> 6) & 63), 0x80 | (code & 63));
      }
    }
    return new Uint8Array(output);
  }

  function utf8Decode(input) {
    const bytes = input instanceof Uint8Array ? input : new Uint8Array(input || []);
    const points = [];
    for (let position = 0; position < bytes.length;) {
      const first = bytes[position++];
      if (first < 0x80) {
        points.push(first);
      } else if ((first & 0xe0) === 0xc0 && position < bytes.length) {
        points.push(((first & 31) << 6) | (bytes[position++] & 63));
      } else if ((first & 0xf0) === 0xe0 && position + 1 < bytes.length) {
        points.push(((first & 15) << 12) | ((bytes[position++] & 63) << 6) | (bytes[position++] & 63));
      } else if ((first & 0xf8) === 0xf0 && position + 2 < bytes.length) {
        points.push(((first & 7) << 18) | ((bytes[position++] & 63) << 12) | ((bytes[position++] & 63) << 6) | (bytes[position++] & 63));
      } else {
        points.push(0xfffd);
      }
    }
    return points.map((point) => String.fromCodePoint(point)).join("");
  }

  class MemoryStorage {
    constructor(keys) {
      this.values = new Map();
      for (const key of Array.isArray(keys) ? keys : []) this.values.set(asString(key), "");
    }
    get length() { return this.values.size; }
    clear() { this.values.clear(); }
    getItem(key) { return this.values.has(asString(key)) ? this.values.get(asString(key)) : null; }
    key(index) { return Array.from(this.values.keys())[Number(index)] ?? null; }
    removeItem(key) { this.values.delete(asString(key)); }
    setItem(key, value) { this.values.set(asString(key), asString(value)); }
  }

  class MinimalSearchParams {
    constructor(query) {
      this.names = asString(query).replace(/^\?/, "").split("&").filter(Boolean).map((entry) => decodeURIComponent(entry.split("=", 1)[0]));
    }
    keys() { return this.names[Symbol.iterator](); }
  }

  function encodeURLText(value) {
    return asString(value).split("%").map((part) => encodeURI(part)).join("%");
  }

  class MinimalURL {
    constructor(value, base) {
      const input = asString(value);
      const fallbackBase = asString(base, "https://chatgpt.com/");
      const parseAbsolute = (target) => {
        const match = /^(https?:)\/\/([^/?#]+)([^?#]*)?(\?[^#]*)?(#.*)?$/i.exec(target);
        if (!match) throw new TypeError("Invalid URL");
        return {
          protocol: match[1].toLowerCase(),
          host: match[2],
          pathname: match[3] || "/",
          search: match[4] || "",
          hash: match[5] || "",
        };
      };
      const splitReference = (target) => {
        const hashIndex = target.indexOf("#");
        const hash = hashIndex >= 0 ? target.slice(hashIndex) : "";
        const withoutHash = hashIndex >= 0 ? target.slice(0, hashIndex) : target;
        const queryIndex = withoutHash.indexOf("?");
        return {
          pathname: queryIndex >= 0 ? withoutHash.slice(0, queryIndex) : withoutHash,
          search: queryIndex >= 0 ? withoutHash.slice(queryIndex) : "",
          hash,
        };
      };
      const normalizePath = (target) => {
        target = target.replace(/\\/g, "/");
        const absolute = target.startsWith("/");
        const trailingSlash = target.endsWith("/") || target.endsWith("/.") || target.endsWith("/..");
        const segments = [];
        const sourceSegments = target.split("/");
        for (let index = 0; index < sourceSegments.length; index++) {
          const segment = sourceSegments[index];
          const lowerSegment = segment.toLowerCase();
          const singleDot = segment === "." || lowerSegment === "%2e";
          const doubleDot = segment === ".." || lowerSegment === ".%2e" || lowerSegment === "%2e." || lowerSegment === "%2e%2e";
          if (singleDot) continue;
          if (doubleDot) {
            if (segments.length > (absolute ? 1 : 0) && segments[segments.length - 1] !== "..") segments.pop();
            else if (!absolute) segments.push(segment);
            continue;
          }
          segments.push(segment);
        }
        let normalized = segments.join("/");
        if (!normalized && absolute) normalized = "/";
        if (trailingSlash && normalized !== "/") normalized += "/";
        return normalized;
      };

      let resolved;
      if (/^https?:\/\//i.test(input)) {
        resolved = parseAbsolute(input);
      } else {
        let baseParts;
        try {
          baseParts = parseAbsolute(fallbackBase);
        } catch (_error) {
          baseParts = parseAbsolute("https://chatgpt.com/");
        }
        if (input.startsWith("//")) {
          resolved = parseAbsolute(`${baseParts.protocol}${input}`);
        } else if (input === "") {
          resolved = baseParts;
        } else {
          const reference = splitReference(input);
          let pathname = baseParts.pathname;
          let search = reference.search;
          if (reference.pathname) {
            if (reference.pathname.startsWith("/")) {
              pathname = reference.pathname;
            } else {
              const directory = pathname.slice(0, pathname.lastIndexOf("/") + 1);
              pathname = `${directory}${reference.pathname}`;
            }
          } else if (!reference.search) {
            search = baseParts.search;
          }
          resolved = {
            protocol: baseParts.protocol,
            host: baseParts.host,
            pathname: normalizePath(pathname),
            search,
            hash: reference.hash,
          };
        }
      }
      resolved.pathname = normalizePath(resolved.pathname);
      this.protocol = resolved.protocol;
      this.host = resolved.host;
      this.hostname = this.host.startsWith("[") ? this.host.slice(1, this.host.indexOf("]")) : this.host.split(":", 1)[0];
      this.pathname = encodeURLText(resolved.pathname);
      this.search = encodeURLText(resolved.search);
      this.hash = encodeURLText(resolved.hash);
      this.origin = `${this.protocol}//${this.host}`;
      this.href = `${this.origin}${this.pathname}${this.search}${this.hash}`;
      this.searchParams = new MinimalSearchParams(this.search);
    }
    toString() { return this.href; }
  }

  function makeElement(name) {
    const upperName = asString(name, "div").toUpperCase();
    const children = [];
    return {
      nodeType: 1,
      nodeName: upperName,
      tagName: upperName,
      style: {},
      children,
      src: "",
      appendChild(child) { children.push(child); return child; },
      removeChild(child) { const index = children.indexOf(child); if (index >= 0) children.splice(index, 1); return child; },
      setAttribute(key, value) { this[asString(key)] = asString(value); },
      getAttribute(key) { const value = this[asString(key)]; return value == null ? null : asString(value); },
      addEventListener: noop,
      removeEventListener: noop,
      dispatchEvent() { return true; },
      getBoundingClientRect() { return { x: 0, y: 0, width: 0, height: 0, top: 0, right: 0, bottom: 0, left: 0 }; },
    };
  }

  const URLImplementation = typeof host.URL === "function" ? host.URL : MinimalURL;
  const location = new URLImplementation(asString(config.location, "https://chatgpt.com/"));
  const width = Number(config.screen_width) || 1920;
  const height = Number(config.screen_height) || 1080;
  const scriptNodes = (Array.isArray(config.script_sources) ? config.script_sources : []).map((source) => {
    const node = makeElement("script");
    node.src = asString(source);
    return node;
  });
  const sdkURL = asString(config.sdk_url);
  let sdkNode = scriptNodes.find((node) => node.src === sdkURL);
  if (!sdkNode) {
    sdkNode = makeElement("script");
    sdkNode.src = asString(config.sdk_url);
    scriptNodes.push(sdkNode);
  }
  const rootElement = makeElement("html");
  rootElement.clientWidth = width;
  rootElement.clientHeight = height;
  const document = {
    readyState: "complete",
    hidden: false,
    visibilityState: "visible",
    referrer: "https://chatgpt.com/",
    URL: location.href,
    cookie: `oai-did=${encodeURIComponent(asString(config.device_id))}`,
    scripts: scriptNodes,
    currentScript: sdkNode || null,
    documentElement: rootElement,
    head: makeElement("head"),
    body: makeElement("body"),
    createElement(name) { const node = makeElement(name); if (node.tagName === "SCRIPT") scriptNodes.push(node); return node; },
    createElementNS(_namespace, name) { return this.createElement(name); },
    querySelector() { return null; },
    querySelectorAll(selector) { return asString(selector).toLowerCase().includes("script") ? scriptNodes.slice() : []; },
    getElementById() { return null; },
    getElementsByTagName(name) { return asString(name).toLowerCase() === "script" ? scriptNodes.slice() : []; },
    addEventListener: noop,
    removeEventListener: noop,
    dispatchEvent() { return true; },
  };

  const entropy = decodeBase64(config.random_b64);
  let entropyPosition = 0;
  function getRandomValues(target) {
    if (!target || typeof target.length !== "number") throw new TypeError("Expected a typed array");
    if (entropyPosition + target.length > entropy.length) throw new Error("Sentinel random pool exhausted");
    target.set(entropy.subarray(entropyPosition, entropyPosition + target.length));
    entropyPosition += target.length;
    return target;
  }
  function randomNumber() {
    const sample = getRandomValues(new Uint8Array(7));
    let integer = 0;
    for (const byte of sample) integer = integer * 256 + byte;
    return (integer % 9007199254740992) / 9007199254740992;
  }
  function randomUUID() {
    const bytes = getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  const fallbackTimeout = (callback) => { if (typeof callback === "function") callback(); return 1; };
  const schedule = typeof host.setTimeout === "function" ? host.setTimeout.bind(host) : fallbackTimeout;
  const cancelScheduled = typeof host.clearTimeout === "function" ? host.clearTimeout.bind(host) : noop;

  delete host.std;
  delete host.os;
  host.std = undefined;
  host.os = undefined;
  host.window = host;
  host.self = host;
  host.top = host;
  host.parent = host;
  host.document = document;
  host.location = location;
  host.navigator = {
    userAgent: asString(config.user_agent, "Mozilla/5.0"),
    language: asString(config.language, "en-US"),
    languages: Array.isArray(config.languages) ? config.languages.map(asString) : ["en-US", "en"],
    hardwareConcurrency: Number(config.hardware_concurrency) || 8,
    platform: asString(config.platform, "MacIntel"),
    vendor: "Google Inc.",
    webdriver: false,
  };
  host.screen = { width, height, availWidth: width, availHeight: height, colorDepth: 24, pixelDepth: 24 };
  host.performance = { now: () => Date.now() % 50000, timeOrigin: Date.now() - (Date.now() % 50000), memory: { jsHeapSizeLimit: 4294967296 } };
  host.localStorage = new MemoryStorage(config.local_storage_keys);
  host.sessionStorage = new MemoryStorage([]);
  host.URL = URLImplementation;
  host.URLSearchParams = typeof host.URLSearchParams === "function" ? host.URLSearchParams : MinimalSearchParams;
  host.TextEncoder = class TextEncoder { encode(value) { return utf8Encode(value); } };
  host.TextDecoder = class TextDecoder { decode(value) { return utf8Decode(value); } };
  host.atob = (value) => Array.from(decodeBase64(value), (byte) => String.fromCharCode(byte)).join("");
  host.btoa = (value) => {
    const input = asString(value);
    const bytes = new Uint8Array(input.length);
    for (let index = 0; index < input.length; index++) {
      const unit = input.charCodeAt(index);
      if (unit > 0xff) throw invalidBase64Error();
      bytes[index] = unit;
    }
    return encodeBase64(bytes);
  };
  host.crypto = { getRandomValues, randomUUID };
  host.Event = host.Event || class Event { constructor(type) { this.type = type; } };
  host.CustomEvent = host.CustomEvent || class CustomEvent extends host.Event { constructor(type, options) { super(type); this.detail = options && "detail" in options ? options.detail : null; } };
  host.MessageChannel = host.MessageChannel || class MessageChannel {
    constructor() {
      const port = () => ({ postMessage: noop, addEventListener: noop, removeEventListener: noop, start: noop, close: noop });
      this.port1 = port();
      this.port2 = port();
    }
  };
  host.setTimeout = schedule;
  host.clearTimeout = cancelScheduled;
  host.setInterval = typeof host.setInterval === "function" ? host.setInterval.bind(host) : () => 1;
  host.clearInterval = typeof host.clearInterval === "function" ? host.clearInterval.bind(host) : noop;
  host.requestIdleCallback = (callback) => schedule(() => callback({ didTimeout: false, timeRemaining: () => 50 }), 0);
  host.cancelIdleCallback = cancelScheduled;
  host.addEventListener = noop;
  host.removeEventListener = noop;
  host.dispatchEvent = () => true;
  host.postMessage = noop;
  host.matchMedia = (query) => ({ media: asString(query), matches: false, onchange: null, addListener: noop, removeListener: noop, addEventListener: noop, removeEventListener: noop, dispatchEvent() { return false; } });
  host.getComputedStyle = () => ({ getPropertyValue() { return ""; } });
  host.history = { length: 1, state: null, back: noop, forward: noop, go: noop, pushState: noop, replaceState: noop };
  host.chrome = { runtime: {}, app: {} };
  host.CSS = { supports() { return true; } };
  host.indexedDB = { open() { return { onerror: null, onsuccess: null, onupgradeneeded: null, result: {}, error: null }; }, deleteDatabase() { return {}; } };
  host.fetch = async () => { throw new Error("Sentinel SDK network access is disabled"); };
  host.__sentinel_init_pending = [];
  host.__sentinel_token_pending = [];
  Math.random = randomNumber;
})(globalThis.__sentinelBootstrap || {}, globalThis);

/*__SENTINEL_SDK__*/

(function publishSentinelBridge(host) {
  "use strict";
  const sdk = host.__sentinelInternals;
  if (!sdk || typeof sdk.D !== "function" || typeof sdk._n !== "function" || typeof sdk.Et !== "function" || typeof sdk.Nt !== "function") {
    throw new Error("Sentinel SDK export adapter is unavailable");
  }
  host.__sentinelBridge = Object.freeze({
    async solveTurnstile(input) {
      const challenge = input && input.challenge ? input.challenge : {};
      sdk.D(challenge, asString(input && input.requirements_token));
      const dx = challenge && challenge.turnstile ? challenge.turnstile.dx : "";
      if (!dx) return "";
      const result = await sdk._n(challenge, dx);
      return result == null ? "" : String(result);
    },
    async startObserver(input) {
      const challenge = input && input.challenge ? input.challenge : {};
      sdk.D(challenge, input && input.requirements_token == null ? "" : String(input.requirements_token));
      const pending = [];
      const originalCatch = Promise.prototype.catch;
      Promise.prototype.catch = function captureCollectorPromise(onRejected) {
        const handled = originalCatch.call(this, onRejected);
        if (!pending.includes(handled)) pending.push(handled);
        return handled;
      };
      let collector;
      try {
        collector = sdk.Et(challenge);
      } finally {
        Promise.prototype.catch = originalCatch;
      }
      if (collector && typeof collector.then === "function" && !pending.includes(collector)) pending.push(collector);
      if (Array.isArray(host.__sentinel_init_pending)) pending.push(...host.__sentinel_init_pending.splice(0));
      if (!pending.length) throw new Error("Sentinel SDK collector adapter is unavailable");
      await Promise.all(pending);
      await Promise.resolve();
      return "ok";
    },
    async snapshotObserver(input) {
      const challenge = input && input.challenge ? input.challenge : {};
      const dx = challenge && challenge.so ? challenge.so.snapshot_dx : "";
      if (!dx) return "";
      const result = await sdk.Nt(dx);
      return result == null ? "" : String(result);
    },
  });

  function asString(value) {
    return value == null ? "" : String(value);
  }
})(globalThis);
