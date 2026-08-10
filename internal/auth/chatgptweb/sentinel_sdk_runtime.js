(function bootstrapSentinelHost(config, host) {
  "use strict";

  const noop = () => {};
  const asString = (value, fallback = "") => value == null ? fallback : String(value);
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  function defineValue(target, key, value, options = {}) {
    Object.defineProperty(target, key, {
      configurable: options.configurable !== false,
      enumerable: options.enumerable === true,
      writable: options.writable !== false,
      value,
    });
    return value;
  }

  function defineReadonly(target, key, value, enumerable = true) {
    Object.defineProperty(target, key, {
      configurable: true,
      enumerable,
      get() { return value; },
    });
  }

  function setObjectTag(target, tag) {
    if (typeof Symbol === "function" && Symbol.toStringTag) {
      Object.defineProperty(target, Symbol.toStringTag, {
        configurable: true,
        value: tag,
      });
    }
  }

  function hideHostProperty(target, key) {
    const descriptor = Object.getOwnPropertyDescriptor(target, key);
    if (descriptor && descriptor.configurable) {
      Object.defineProperty(target, key, { ...descriptor, enumerable: false });
    }
  }

  hideHostProperty(host, "__sentinelBootstrap");

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
      defineValue(this, "values", new Map(), { enumerable: false });
      for (const key of Array.isArray(keys) ? keys : []) this.setItem(key, "");
    }
    get length() { return this.values.size; }
    clear() {
      for (const key of this.values.keys()) this.removeNamedProperty(key);
      this.values.clear();
    }
    getItem(key) { return this.values.has(asString(key)) ? this.values.get(asString(key)) : null; }
    key(index) { return Array.from(this.values.keys())[Number(index)] ?? null; }
    removeItem(key) {
      const name = asString(key);
      this.values.delete(name);
      this.removeNamedProperty(name);
    }
    setItem(key, value) {
      const name = asString(key);
      this.values.set(name, asString(value));
      if (name === "values" || name in MemoryStorage.prototype) return;
      if (!Object.prototype.hasOwnProperty.call(this, name)) {
        Object.defineProperty(this, name, {
          configurable: true,
          enumerable: true,
          get: () => this.values.get(name),
          set: (next) => this.values.set(name, asString(next)),
        });
      }
    }
    removeNamedProperty(name) {
      if (name !== "values" && Object.prototype.hasOwnProperty.call(this, name)) delete this[name];
    }
  }
  setObjectTag(MemoryStorage.prototype, "Storage");

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

  const elementState = new WeakMap();
  const contextState = new WeakMap();
  const elementPrototype = {};
  const htmlElementPrototype = Object.create(elementPrototype);
  const canvasElementPrototype = Object.create(htmlElementPrototype);
  const canvas2DPrototype = {};
  const webGLPrototype = {};
  const webGLDebugRendererInfo = {};
  setObjectTag(elementPrototype, "Element");
  setObjectTag(htmlElementPrototype, "HTMLElement");
  setObjectTag(canvasElementPrototype, "HTMLCanvasElement");
  setObjectTag(canvas2DPrototype, "CanvasRenderingContext2D");
  setObjectTag(webGLPrototype, "WebGLRenderingContext");

  const elementData = (element) => elementState.get(element) || {
    tagName: "DIV",
    attributes: new Map(),
    children: [],
    style: {},
    src: "",
    width: 0,
    height: 0,
    contexts: new Map(),
  };
  defineReadonly(elementPrototype, "nodeType", 1);
  for (const key of ["nodeName", "tagName"]) {
    Object.defineProperty(elementPrototype, key, {
      configurable: true,
      enumerable: true,
      get() { return elementData(this).tagName; },
    });
  }
  for (const key of ["style", "children", "childNodes"]) {
    Object.defineProperty(elementPrototype, key, {
      configurable: true,
      enumerable: true,
      get() { return key === "style" ? elementData(this).style : elementData(this).children; },
    });
  }
  Object.defineProperty(elementPrototype, "src", {
    configurable: true,
    enumerable: true,
    get() { return elementData(this).src; },
    set(value) { elementData(this).src = asString(value); },
  });
  defineValue(elementPrototype, "appendChild", function appendChild(child) {
    elementData(this).children.push(child);
    return child;
  }, { enumerable: true });
  defineValue(elementPrototype, "removeChild", function removeChild(child) {
    const children = elementData(this).children;
    const index = children.indexOf(child);
    if (index >= 0) children.splice(index, 1);
    return child;
  }, { enumerable: true });
  defineValue(elementPrototype, "setAttribute", function setAttribute(key, value) {
    const name = asString(key);
    const text = asString(value);
    elementData(this).attributes.set(name, text);
    if (name === "src") this.src = text;
  }, { enumerable: true });
  defineValue(elementPrototype, "getAttribute", function getAttribute(key) {
    const name = asString(key);
    if (name === "src" && this.src) return this.src;
    return elementData(this).attributes.get(name) ?? null;
  }, { enumerable: true });
  defineValue(elementPrototype, "addEventListener", noop, { enumerable: true });
  defineValue(elementPrototype, "removeEventListener", noop, { enumerable: true });
  defineValue(elementPrototype, "dispatchEvent", function dispatchEvent() { return true; }, { enumerable: true });
  defineValue(elementPrototype, "getBoundingClientRect", function getBoundingClientRect() {
    const state = elementData(this);
    const elementWidth = state.width || 0;
    const elementHeight = state.height || 0;
    return { x: 0, y: 0, width: elementWidth, height: elementHeight, top: 0, right: elementWidth, bottom: elementHeight, left: 0 };
  }, { enumerable: true });

  for (const key of ["width", "height"]) {
    Object.defineProperty(canvasElementPrototype, key, {
      configurable: true,
      enumerable: true,
      get() { return elementData(this)[key]; },
      set(value) {
        const number = Number(value);
        elementData(this)[key] = Number.isFinite(number) && number >= 0 ? Math.floor(number) : 0;
      },
    });
  }
  defineValue(canvasElementPrototype, "getContext", function getContext(kind) {
    const normalized = asString(kind, "2d").toLowerCase();
    if (!["2d", "webgl", "experimental-webgl", "webgl2"].includes(normalized)) return null;
    const state = elementData(this);
    if (state.contexts.has(normalized)) return state.contexts.get(normalized);
    const context = Object.create(normalized === "2d" ? canvas2DPrototype : webGLPrototype);
    contextState.set(context, { canvas: this, kind: normalized });
    state.contexts.set(normalized, context);
    return context;
  }, { enumerable: true });
  defineValue(canvasElementPrototype, "toDataURL", function toDataURL(type) {
    const mime = asString(type, "image/png") || "image/png";
    const state = elementData(this);
    const fingerprint = `sentinel-canvas-${config.fingerprint_version}:${config.fingerprint_catalog}:${state.width}x${state.height}`;
    return `data:${mime};base64,${encodeBase64(utf8Encode(fingerprint))}`;
  }, { enumerable: true });

  for (const name of ["fillRect", "clearRect", "drawImage", "save", "restore"]) {
    defineValue(canvas2DPrototype, name, noop, { enumerable: true });
  }
  defineValue(canvas2DPrototype, "measureText", function measureText(value) {
    return { width: asString(value).length * 7.5 };
  }, { enumerable: true });
  Object.defineProperty(canvas2DPrototype, "canvas", {
    configurable: true,
    enumerable: true,
    get() { return (contextState.get(this) || {}).canvas || null; },
  });

  defineReadonly(webGLDebugRendererInfo, "UNMASKED_VENDOR_WEBGL", 0x9245);
  defineReadonly(webGLDebugRendererInfo, "UNMASKED_RENDERER_WEBGL", 0x9246);
  defineValue(webGLPrototype, "getExtension", function getExtension(name) {
    return asString(name).toLowerCase() === "webgl_debug_renderer_info" ? webGLDebugRendererInfo : null;
  }, { enumerable: true });
  defineValue(webGLPrototype, "getParameter", function getParameter(parameter) {
    switch (Number(parameter)) {
      case 0x1f00: return "WebKit";
      case 0x1f01: return "WebKit WebGL";
      case 0x1f02: return "WebGL 1.0 (OpenGL ES 2.0 Chromium)";
      case 0x8b8c: return "WebGL GLSL ES 1.0 (OpenGL ES GLSL ES 1.0 Chromium)";
      case 0x9245: return asString(config.webgl_vendor, "Google Inc.");
      case 0x9246: return asString(config.webgl_renderer, "ANGLE (Google, Vulkan, Vulkan)");
      default: return 0;
    }
  }, { enumerable: true });
  defineValue(webGLPrototype, "getSupportedExtensions", function getSupportedExtensions() {
    return ["WEBGL_debug_renderer_info"];
  }, { enumerable: true });
  Object.defineProperty(webGLPrototype, "canvas", {
    configurable: true,
    enumerable: true,
    get() { return (contextState.get(this) || {}).canvas || null; },
  });
  for (const [key, value] of Object.entries({
    VENDOR: 0x1f00,
    RENDERER: 0x1f01,
    VERSION: 0x1f02,
    SHADING_LANGUAGE_VERSION: 0x8b8c,
  })) defineReadonly(webGLPrototype, key, value);

  function makeElement(name) {
    const upperName = asString(name, "div").toUpperCase();
    const canvas = upperName === "CANVAS";
    const element = Object.create(canvas ? canvasElementPrototype : htmlElementPrototype);
    elementState.set(element, {
      tagName: upperName,
      attributes: new Map(),
      children: [],
      style: {},
      src: "",
      width: canvas ? 300 : 0,
      height: canvas ? 150 : 0,
      contexts: new Map(),
    });
    return element;
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
  defineValue(rootElement, "clientWidth", width, { enumerable: true });
  defineValue(rootElement, "clientHeight", height, { enumerable: true });
  const documentPrototype = {};
  setObjectTag(documentPrototype, "HTMLDocument");
  const document = Object.create(documentPrototype);
  defineReadonly(document, "readyState", "complete", false);
  defineReadonly(document, "hidden", false, false);
  defineReadonly(document, "visibilityState", "visible", false);
  defineReadonly(document, "referrer", "https://chatgpt.com/", false);
  defineReadonly(document, "URL", location.href, false);
  defineReadonly(document, "location", location, false);
  defineValue(document, "cookie", `oai-did=${encodeURIComponent(asString(config.device_id))}`, { enumerable: false });
  defineReadonly(document, "scripts", scriptNodes, false);
  defineReadonly(document, "currentScript", sdkNode || null, false);
  defineReadonly(document, "documentElement", rootElement, false);
  defineReadonly(document, "head", makeElement("head"), false);
  defineReadonly(document, "body", makeElement("body"), false);
  defineValue(documentPrototype, "createElement", function createElement(name) {
    const node = makeElement(name);
    if (node.tagName === "SCRIPT") scriptNodes.push(node);
    return node;
  }, { enumerable: true });
  defineValue(documentPrototype, "createElementNS", function createElementNS(_namespace, name) {
    return this.createElement(name);
  }, { enumerable: true });
  defineValue(documentPrototype, "querySelector", function querySelector() { return null; }, { enumerable: true });
  defineValue(documentPrototype, "querySelectorAll", function querySelectorAll(selector) {
    return asString(selector).toLowerCase().includes("script") ? scriptNodes.slice() : [];
  }, { enumerable: true });
  defineValue(documentPrototype, "getElementById", function getElementById() { return null; }, { enumerable: true });
  defineValue(documentPrototype, "getElementsByTagName", function getElementsByTagName(name) {
    return asString(name).toLowerCase() === "script" ? scriptNodes.slice() : [];
  }, { enumerable: true });
  defineValue(documentPrototype, "addEventListener", noop, { enumerable: true });
  defineValue(documentPrototype, "removeEventListener", noop, { enumerable: true });
  defineValue(documentPrototype, "dispatchEvent", function dispatchEvent() { return true; }, { enumerable: true });

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

  function randomBase36(length) {
    const symbols = "abcdefghijklmnopqrstuvwxyz0123456789";
    const bytes = getRandomValues(new Uint8Array(length));
    return Array.from(bytes, (byte) => symbols[byte % symbols.length]).join("");
  }
  const reactDocumentKey = `__reactContainer$${randomBase36(11)}`;
  defineValue(document, reactDocumentKey, reactDocumentKey, { enumerable: true });

  const fallbackTimeout = (callback) => { if (typeof callback === "function") callback(); return 1; };
  const schedule = typeof host.setTimeout === "function" ? host.setTimeout.bind(host) : fallbackTimeout;
  const cancelScheduled = typeof host.clearTimeout === "function" ? host.clearTimeout.bind(host) : noop;

  delete host.std;
  delete host.os;
  host.std = undefined;
  host.os = undefined;
  defineReadonly(host, "window", host, false);
  defineReadonly(host, "self", host, false);
  defineReadonly(host, "top", host, false);
  defineReadonly(host, "parent", host, false);
  defineReadonly(host, "document", document, false);
  defineReadonly(host, "location", location, false);

  const navigatorPrototype = {};
  setObjectTag(navigatorPrototype, "Navigator");
  const navigator = Object.create(navigatorPrototype);
  const pluginArray = {};
  const mimeTypeArray = {};
  setObjectTag(pluginArray, "PluginArray");
  setObjectTag(mimeTypeArray, "MimeTypeArray");
  defineReadonly(pluginArray, "length", 0, false);
  defineReadonly(mimeTypeArray, "length", 0, false);
  const userAgent = asString(config.user_agent, "Mozilla/5.0");
  const navigatorValues = {
    userAgent,
    appVersion: userAgent.replace(/^Mozilla\//, ""),
    appCodeName: "Mozilla",
    appName: "Netscape",
    language: asString(config.language, "en-US"),
    languages: Array.isArray(config.languages) ? config.languages.map((value) => asString(value)) : ["en-US", "en"],
    hardwareConcurrency: Number(config.hardware_concurrency) || 8,
    deviceMemory: Number(config.device_memory) || 8,
    maxTouchPoints: Number(config.max_touch_points) || 0,
    platform: asString(config.platform, "MacIntel"),
    vendor: "Google Inc.",
    vendorSub: "",
    product: "Gecko",
    productSub: "20030107",
    webdriver: false,
    cookieEnabled: true,
    onLine: true,
    pdfViewerEnabled: true,
    plugins: pluginArray,
    mimeTypes: mimeTypeArray,
  };
  for (const [key, value] of Object.entries(navigatorValues)) defineReadonly(navigatorPrototype, key, value);
  defineReadonly(host, "navigator", navigator, false);

  const screenPrototype = {};
  setObjectTag(screenPrototype, "Screen");
  const screen = Object.create(screenPrototype);
  const screenValues = {
    width,
    height,
    availLeft: Number(config.screen_avail_left) || 0,
    availTop: Number(config.screen_avail_top) || 0,
    availWidth: Number(config.screen_avail_width) || width,
    availHeight: Number(config.screen_avail_height) || height,
    colorDepth: Number(config.screen_color_depth) || 24,
    pixelDepth: Number(config.screen_color_depth) || 24,
  };
  for (const [key, value] of Object.entries(screenValues)) defineReadonly(screenPrototype, key, value);
  defineReadonly(host, "screen", screen, false);

  const pageStartedAt = Number(config.page_started_at_ms);
  const timeOrigin = Number.isFinite(pageStartedAt) && pageStartedAt > 0 ? pageStartedAt : Date.now();
  const performancePrototype = {};
  setObjectTag(performancePrototype, "Performance");
  const performance = Object.create(performancePrototype);
  defineReadonly(performance, "timeOrigin", timeOrigin, false);
  defineReadonly(performance, "memory", { jsHeapSizeLimit: Number(config.js_heap_size_limit) || 4294967296 }, false);
  defineValue(performancePrototype, "now", function now() { return Math.max(0, Date.now() - timeOrigin); }, { enumerable: true });
  defineReadonly(host, "performance", performance, false);
  defineReadonly(host, "devicePixelRatio", Number(config.device_pixel_ratio) || 1, false);
  defineReadonly(host, "innerWidth", Number(config.inner_width) || width, false);
  defineReadonly(host, "innerHeight", Number(config.inner_height) || height, false);
  defineReadonly(host, "outerWidth", Number(config.outer_width) || width, false);
  defineReadonly(host, "outerHeight", Number(config.outer_height) || height, false);

  const localStorage = new MemoryStorage(config.local_storage_keys);
  const sessionStorage = new MemoryStorage([]);
  defineReadonly(host, "localStorage", localStorage, false);
  defineReadonly(host, "sessionStorage", sessionStorage, false);
  defineValue(host, "__reactRouterContext", {
    basename: "/",
    future: {},
    isSpaMode: true,
    routeDiscovery: {},
    ssr: false,
    state: undefined,
  }, { enumerable: false });

  const ElementConstructor = function Element() {};
  const HTMLElementConstructor = function HTMLElement() {};
  const HTMLCanvasElementConstructor = function HTMLCanvasElement() {};
  const DocumentConstructor = function Document() {};
  const NavigatorConstructor = function Navigator() {};
  const ScreenConstructor = function Screen() {};
  const CanvasRenderingContext2DConstructor = function CanvasRenderingContext2D() {};
  const WebGLRenderingContextConstructor = function WebGLRenderingContext() {};
  ElementConstructor.prototype = elementPrototype;
  HTMLElementConstructor.prototype = htmlElementPrototype;
  HTMLCanvasElementConstructor.prototype = canvasElementPrototype;
  DocumentConstructor.prototype = documentPrototype;
  NavigatorConstructor.prototype = navigatorPrototype;
  ScreenConstructor.prototype = screenPrototype;
  CanvasRenderingContext2DConstructor.prototype = canvas2DPrototype;
  WebGLRenderingContextConstructor.prototype = webGLPrototype;
  defineValue(elementPrototype, "constructor", ElementConstructor, { enumerable: false });
  defineValue(htmlElementPrototype, "constructor", HTMLElementConstructor, { enumerable: false });
  defineValue(canvasElementPrototype, "constructor", HTMLCanvasElementConstructor, { enumerable: false });
  defineValue(documentPrototype, "constructor", DocumentConstructor, { enumerable: false });
  defineValue(navigatorPrototype, "constructor", NavigatorConstructor, { enumerable: false });
  defineValue(screenPrototype, "constructor", ScreenConstructor, { enumerable: false });
  defineValue(canvas2DPrototype, "constructor", CanvasRenderingContext2DConstructor, { enumerable: false });
  defineValue(webGLPrototype, "constructor", WebGLRenderingContextConstructor, { enumerable: false });
  defineValue(host, "Element", ElementConstructor, { enumerable: false });
  defineValue(host, "HTMLElement", HTMLElementConstructor, { enumerable: false });
  defineValue(host, "HTMLCanvasElement", HTMLCanvasElementConstructor, { enumerable: false });
  defineValue(host, "Document", DocumentConstructor, { enumerable: false });
  defineValue(host, "Navigator", NavigatorConstructor, { enumerable: false });
  defineValue(host, "Screen", ScreenConstructor, { enumerable: false });
  defineValue(host, "Storage", MemoryStorage, { enumerable: false });
  defineValue(host, "CanvasRenderingContext2D", CanvasRenderingContext2DConstructor, { enumerable: false });
  defineValue(host, "WebGLRenderingContext", WebGLRenderingContextConstructor, { enumerable: false });
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
  defineValue(host, "__sentinel_init_pending", [], { enumerable: false });
  defineValue(host, "__sentinel_token_pending", [], { enumerable: false });
  Math.random = randomNumber;
})(globalThis.__sentinelBootstrap || {}, globalThis);

/*__SENTINEL_SDK__*/

(function publishSentinelBridge(host) {
  "use strict";
  const sdk = host.__sentinelInternals;
  if (!sdk || typeof sdk.D !== "function" || typeof sdk._n !== "function" || typeof sdk.Et !== "function" || typeof sdk.Nt !== "function") {
    throw new Error("Sentinel SDK export adapter is unavailable");
  }
  for (const key of ["__sentinelBootstrap", "__sentinelInternals", "__sentinel_init_pending", "__sentinel_token_pending"]) {
    const descriptor = Object.getOwnPropertyDescriptor(host, key);
    if (descriptor && descriptor.configurable) Object.defineProperty(host, key, { ...descriptor, enumerable: false });
  }
  const bridge = Object.freeze({
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
  Object.defineProperty(host, "__sentinelBridge", {
    configurable: true,
    enumerable: false,
    writable: false,
    value: bridge,
  });

  function asString(value) {
    return value == null ? "" : String(value);
  }
})(globalThis);
