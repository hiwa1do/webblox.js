import { connect } from "cloudflare:sockets";

const DEFAULT_WISP_PATH = "/wisp/";

const SCRAMJET_REPO = "https://cdn.jsdelivr.net/gh/hiwa1do/scramjet-browser@HEAD";
const WEBBLOX_REPO = "https://cdn.jsdelivr.net/gh/x8rr/webblox-built@main/webblox";
const EPOXY_URL = "https://cdn.jsdelivr.net/npm/@mercuryworkshop/epoxy-transport@3/dist/index.mjs";
const ROBLOX_VERSION_URL = "https://clientsettings.roblox.com/v2/client-version/WebClientApp";
const SCRAMJET_COOKIE_DB = "__hmxiqeqbgz8_thmxiqe";

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".htm": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
  ".otf": "font/otf",
  ".ttf": "font/ttf",
  ".woff": "font/woff",
  ".woff2": "font/woff2"
};

let cachedEngineVersion = null;
let engineVersionExpiry = 0;

const QUEUE_SIZE = 128;
const CONTINUE_EVERY = QUEUE_SIZE / 4;
const HEADER_SIZE = 5;
const EMPTY = new Uint8Array(0);

function makePacket(type, streamId, payload = EMPTY) {
  const packet = new Uint8Array(HEADER_SIZE + payload.length);
  packet[0] = type;
  new DataView(packet.buffer).setUint32(1, streamId >>> 0, true);
  packet.set(payload, HEADER_SIZE);
  return packet;
}

function makeUint32Payload(value) {
  const payload = new Uint8Array(4);
  new DataView(payload.buffer).setUint32(0, value >>> 0, true);
  return payload;
}

function makeClosePayload(reason) {
  return new Uint8Array([reason & 0xff]);
}

async function messageBytes(data) {
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }

  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(
      data.buffer,
      data.byteOffset,
      data.byteLength
    );
  }

  if (typeof Blob !== "undefined" && data instanceof Blob) {
    return new Uint8Array(await data.arrayBuffer());
  }

  return null;
}

class WispSession {
  constructor(websocket) {
    this.websocket = websocket;
    this.streams = new Map();
    this.closed = false;
    this.incoming = Promise.resolve();
  }

  send(type, streamId, payload = EMPTY) {
    if (
      this.closed ||
      this.websocket.readyState !== WebSocket.OPEN
    ) {
      return false;
    }

    try {
      this.websocket.send(makePacket(type, streamId, payload));
      return true;
    } catch (error) {
      console.error("Wisp WebSocket send failed:", error);
      this.shutdown(1011, "WebSocket send failed");
      return false;
    }
  }

  sendContinue(streamId, remaining) {
    this.send(0x03, streamId, makeUint32Payload(remaining));
  }

  sendClose(streamId, reason) {
    this.send(0x04, streamId, makeClosePayload(reason));
  }

  attach() {
    this.websocket.binaryType = "arraybuffer";

    this.websocket.addEventListener("message", event => {
      this.incoming = this.incoming
        .then(() => this.handleMessage(event.data))
        .catch(error => {
          console.error("Wisp message handler failed:", error);
          this.shutdown(1011, "Internal Wisp error");
        });
    });

    this.websocket.addEventListener("close", () => {
      this.shutdown(1000, "WebSocket closed", false);
    });

    this.websocket.addEventListener("error", () => {
      this.shutdown(1011, "WebSocket error", false);
    });

    this.sendContinue(0, QUEUE_SIZE);
  }

  async handleMessage(raw) {
    if (this.closed) return;

    const data = await messageBytes(raw);

    if (!data) return;

    if (data.byteLength < HEADER_SIZE) {
      this.shutdown(1002, "Malformed Wisp packet");
      return;
    }

    const type = data[0];

    const streamId = new DataView(
      data.buffer,
      data.byteOffset,
      data.byteLength
    ).getUint32(1, true);

    const payload = data.subarray(HEADER_SIZE);

    if (type === 0x01) {
      this.handleConnect(streamId, payload);
      return;
    }

    if (type === 0x02) {
      const stream = this.streams.get(streamId);

      if (stream) {
        await this.enqueue(stream, payload.slice());
      }

      return;
    }

    if (type === 0x04) {
      this.closeStream(streamId);
    }
  }

  handleConnect(streamId, payload) {
    if (
      streamId === 0 ||
      this.streams.has(streamId) ||
      payload.byteLength < 4
    ) {
      this.sendClose(streamId, 0x41);
      return;
    }

    const view = new DataView(
      payload.buffer,
      payload.byteOffset,
      payload.byteLength
    );

    const streamType = payload[0];
    const port = view.getUint16(1, true);

    let hostname;

    try {
      hostname = new TextDecoder("utf-8", {
        fatal: true
      }).decode(payload.subarray(3));
    } catch {
      this.sendClose(streamId, 0x41);
      return;
    }

    if (
      streamType !== 0x01 ||
      !hostname ||
      hostname.length > 253 ||
      /[\s\u0000-\u001f\u007f/\\]/.test(hostname) ||
      port < 1 ||
      port > 65535
    ) {
      this.sendClose(streamId, 0x41);
      return;
    }

    const stream = {
      id: streamId,
      hostname,
      port,
      socket: null,
      reader: null,
      writer: null,
      queue: [],
      dataWaiters: [],
      spaceWaiters: [],
      packetsSent: 0,
      closed: false
    };

    this.streams.set(streamId, stream);
    this.openTcpStream(stream);
  }

  async openTcpStream(stream) {
    try {
      const socket = connect(
        {
          hostname: stream.hostname,
          port: stream.port
        },
        {
          secureTransport: "off",
          allowHalfOpen: true
        }
      );

      stream.socket = socket;

      await socket.opened;

      if (this.closed || stream.closed) {
        await socket.close().catch(() => {});
        return;
      }

      stream.reader = socket.readable.getReader();
      stream.writer = socket.writable.getWriter();

      this.pumpClientToTcp(stream);
      this.pumpTcpToClient(stream);
    } catch (error) {
      console.warn(
        `Wisp TCP connect failed (${stream.hostname}:${stream.port}):`,
        error
      );

      if (!stream.closed) {
        this.sendClose(stream.id, 0x42);
        this.closeStream(stream.id);
      }

      if (stream.socket) {
        await stream.socket.close().catch(() => {});
      }
    }
  }

  async enqueue(stream, data) {
    while (
      stream.queue.length >= QUEUE_SIZE &&
      !stream.closed
    ) {
      await new Promise(resolve => {
        stream.spaceWaiters.push(resolve);
      });
    }

    if (stream.closed) return;

    stream.queue.push(data);

    const waiter = stream.dataWaiters.shift();

    if (waiter) waiter();
  }

  async dequeue(stream) {
    while (
      stream.queue.length === 0 &&
      !stream.closed
    ) {
      await new Promise(resolve => {
        stream.dataWaiters.push(resolve);
      });
    }

    if (stream.closed || stream.queue.length === 0) {
      return null;
    }

    const data = stream.queue.shift();
    const waiter = stream.spaceWaiters.shift();

    if (waiter) waiter();

    return data;
  }

  wakeStream(stream) {
    for (const resolve of stream.dataWaiters.splice(0)) {
      resolve();
    }

    for (const resolve of stream.spaceWaiters.splice(0)) {
      resolve();
    }
  }

  async pumpClientToTcp(stream) {
    try {
      while (!stream.closed) {
        const data = await this.dequeue(stream);

        if (data === null) return;

        await stream.writer.write(data);

        stream.packetsSent++;

        if (
          stream.packetsSent % CONTINUE_EVERY === 0 &&
          !stream.closed
        ) {
          this.sendContinue(
            stream.id,
            QUEUE_SIZE - stream.queue.length
          );
        }
      }
    } catch (error) {
      if (!stream.closed) {
        console.warn(
          `Wisp TCP write failed (${stream.hostname}:${stream.port}):`,
          error
        );

        this.sendClose(stream.id, 0x03);
        this.closeStream(stream.id);
      }
    }
  }

  async pumpTcpToClient(stream) {
    try {
      while (!stream.closed) {
        const result = await stream.reader.read();

        if (result.done) break;

        const chunk = result.value;

        if (chunk && chunk.byteLength > 0) {
          if (!this.send(0x02, stream.id, chunk)) {
            return;
          }
        }
      }

      if (!stream.closed) {
        this.sendClose(stream.id, 0x02);
        this.closeStream(stream.id);
      }
    } catch (error) {
      if (!stream.closed) {
        console.warn(
          `Wisp TCP read failed (${stream.hostname}:${stream.port}):`,
          error
        );

        this.sendClose(stream.id, 0x03);
        this.closeStream(stream.id);
      }
    }
  }

  closeStream(streamId) {
    const stream = this.streams.get(streamId);

    if (!stream) return;

    this.streams.delete(streamId);

    stream.closed = true;
    stream.queue.length = 0;

    this.wakeStream(stream);

    if (stream.socket) {
      stream.socket.close().catch(() => {});
    }
  }

  shutdown(
    code = 1000,
    reason = "Closed",
    closeWebSocket = true
  ) {
    if (this.closed) return;

    this.closed = true;

    for (const streamId of Array.from(this.streams.keys())) {
      this.closeStream(streamId);
    }

    if (
      closeWebSocket &&
      this.websocket.readyState === WebSocket.OPEN
    ) {
      try {
        this.websocket.close(code, reason.slice(0, 100));
      } catch {}
    }
  }
}

async function serveWisp(request) {
  const url = new URL(request.url);

  if (
    request.headers.get("Upgrade")?.toLowerCase() !== "websocket"
  ) {
    return new Response(
      '<!DOCTYPE html><html><head><title>wisp.js Server</title><style>* {font-family:sans-serif;} body {max-width:700px;}</style></head><body><h1>wisp.js Server</h1><p>This website hosts a public <a href="https://github.com/MercuryWorkshop/wisp-protocol">Wisp</a> server.</p><p>Running wisp.js.</p></body></html>',
      {
        status: 200,
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store"
        }
      }
    );
  }

  if (!url.pathname.endsWith("/")) {
    return new Response(
      "Wisp endpoint paths must end with a slash.\n",
      { status: 404 }
    );
  }

  const pair = new WebSocketPair();

  const client = pair[0];
  const server = pair[1];

  server.accept();

  const session = new WispSession(server);
  session.attach();

  return new Response(null, {
    status: 101,
    webSocket: client
  });
}

function getMime(path) {
  const name = path.split("/").pop() || "";
  const dot = name.lastIndexOf(".");

  if (dot < 0) return "application/octet-stream";

  return MIME_TYPES[name.slice(dot).toLowerCase()] ||
    "application/octet-stream";
}

function makeHeaders(extra = {}, isolated = false) {
  const headers = new Headers(extra);

  headers.set("X-Content-Type-Options", "nosniff");
  headers.set("Referrer-Policy", "strict-origin-when-cross-origin");
  headers.set("Cross-Origin-Resource-Policy", "cross-origin");

  if (isolated) {
    headers.set("Cross-Origin-Opener-Policy", "same-origin");
    headers.set("Cross-Origin-Embedder-Policy", "require-corp");
  }

  return headers;
}

function makeResponse(
  body,
  status = 200,
  extra = {},
  request = null,
  isolated = false
) {
  const noBody =
    request?.method === "HEAD" ||
    status === 204 ||
    status === 205 ||
    status === 304;

  return new Response(noBody ? null : body, {
    status,
    headers: makeHeaders(extra, isolated)
  });
}

async function proxy(target, request, overrides = {}, isolated = false) {
  const requestHeaders = new Headers();

  for (const name of [
    "Accept",
    "Range",
    "If-None-Match",
    "If-Modified-Since"
  ]) {
    const value = request.headers.get(name);

    if (value) requestHeaders.set(name, value);
  }

  const upstream = await fetch(target, {
    method: request.method === "HEAD" ? "HEAD" : "GET",
    headers: requestHeaders,
    redirect: "follow"
  });

  const outputHeaders = {};

  for (const name of [
    "Content-Type",
    "Content-Range",
    "Accept-Ranges",
    "ETag",
    "Last-Modified",
    "Cache-Control",
    "Expires"
  ]) {
    const value = upstream.headers.get(name);

    if (value) outputHeaders[name] = value;
  }

  Object.assign(outputHeaders, overrides);

  const noBody =
    request.method === "HEAD" ||
    upstream.status === 204 ||
    upstream.status === 205 ||
    upstream.status === 304;

  return new Response(noBody ? null : upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: makeHeaders(outputHeaders, isolated)
  });
}

const PLAY_PLUGIN = String.raw`
function createBloxLaunchPlugin(ManagedPlugin) {
  const ID = /^[1-9]\d{0,18}$/;
  const SERVER_ID = /^[A-Za-z0-9-]{1,64}$/;
  const GAME_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

  const ROBLOX_HOST = /^(?:(?:play|www)\.)?roblox\.com$|^play\.sitetest[1-3]\.robloxlabs\.com$/i;
  const LAUNCHER_HOST = /(^|\.)(roblox\.com|robloxlabs\.com)$/i;
  const PROTOCOL = /^(roblox-player|roblox-client|roblox)(?:-sitetest([1-3]))?:/i;

  const MESSAGES = {
    "popup-blocked": "Allow pop-ups for this site, then press Play again.",
    "not-playing": "Your friend isn't in a game you can join.",
    "unreachable": "Couldn't reach Roblox to find your friend.",
    "unsupported": "This Roblox launch type is not supported."
  };

  function param(params, name) {
    const wanted = name.toLowerCase();

    for (const [key, value] of params) {
      if (key.toLowerCase() === wanted) return value;
    }

    return undefined;
  }

  function makeLaunch(placeId, serverId) {
    if (placeId === undefined || !ID.test(String(placeId))) {
      return null;
    }

    const result = {
      placeId: String(placeId)
    };

    if (serverId && SERVER_ID.test(String(serverId))) {
      result.serverId = String(serverId);
    }

    return result;
  }

  function parseGamesStart(raw) {
    let url;

    try {
      url = new URL(raw);
    } catch {
      return null;
    }

    if (url.protocol !== "https:" && url.protocol !== "http:") return null;
    if (!ROBLOX_HOST.test(url.hostname)) return null;
    if (url.port || url.username || url.password) return null;

    if (url.pathname.replace(/\/+$/, "").toLowerCase() !== "/games/start") {
      return null;
    }

    return makeLaunch(
      param(url.searchParams, "placeId"),
      param(url.searchParams, "gameInstanceId")
    );
  }

  function parseLauncherProtocol(raw) {
    const match = PROTOCOL.exec(raw);

    if (!match) return null;

    const domain = match[2]
      ? "sitetest" + match[2] + ".robloxlabs.com"
      : "roblox.com";

    const fields = new Map();

    for (const part of raw.slice(match[0].length).split("+")) {
      const colon = part.indexOf(":");

      if (colon > 0) {
        fields.set(
          part.slice(0, colon).toLowerCase(),
          part.slice(colon + 1)
        );
      }
    }

    if ((fields.get("launchmode") || "play").toLowerCase() !== "play") {
      return null;
    }

    const target = fields.get("placelauncherurl");

    if (!target) return null;

    let launcher;

    try {
      launcher = new URL(decodeURIComponent(target));
    } catch {
      return null;
    }

    if (
      (launcher.protocol !== "https:" && launcher.protocol !== "http:") ||
      !LAUNCHER_HOST.test(launcher.hostname) ||
      !/\/game\/placelauncher\.ashx$/i.test(launcher.pathname)
    ) {
      return null;
    }

    return {
      domain,
      launcher
    };
  }

  function parseLaunchUri(raw) {
    if (/^roblox:\/\//i.test(raw)) {
      const match =
        /^roblox:\/\/(?:experiences\/start\??|navigation\/game_details\??|)(.*)$/i.exec(raw);

      if (!match) return null;

      const params = new URLSearchParams(match[1]);

      return makeLaunch(
        param(params, "placeId"),
        param(params, "gameInstanceId") || param(params, "gameId")
      );
    }

    const protocol = parseLauncherProtocol(raw);

    if (!protocol) return null;

    const params = protocol.launcher.searchParams;
    const request = param(params, "request");
    const placeId = param(params, "placeId");

    if (request === "RequestGame") {
      return makeLaunch(placeId);
    }

    if (request === "RequestGameJob") {
      const serverId =
        param(params, "gameId") ||
        param(params, "jobId");

      return serverId ? makeLaunch(placeId, serverId) : null;
    }

    return null;
  }

  function parseFollowUri(raw) {
    const protocol = parseLauncherProtocol(raw);

    if (!protocol) return null;

    const params = protocol.launcher.searchParams;

    if (param(params, "request") !== "RequestFollowUser") {
      return null;
    }

    const userId = param(params, "userId");

    if (!userId || !ID.test(userId)) return null;

    return {
      userId,
      domain: protocol.domain
    };
  }

  function bloxUrl(launch) {
    const url = new URL("/blox/", location.href);

    url.searchParams.set("placeId", launch.placeId);

    if (launch.serverId) {
      url.searchParams.set("gameInstanceId", launch.serverId);
    }

    const wisp = new URL("${DEFAULT_WISP_PATH}", location.href);
    wisp.protocol = location.protocol === "https:" ? "wss:" : "ws:";

    url.searchParams.set("wisp", wisp.href);

    return url.href;
  }

  function getCurrentPlaceId(win, client) {
    const candidates = [win.location.href];

    try {
      const original = client.unrewriteUrl(win.location.href);

      if (original && original !== win.location.href) {
        candidates.unshift(original);
      }
    } catch {}

    for (const candidate of candidates) {
      try {
        const parsed = new URL(candidate);
        const match = parsed.pathname.match(/^\/games\/([1-9]\d{0,18})(?:\/|$)/i);

        if (match) return match[1];
      } catch {}
    }

    const meta = win.document.querySelector("[data-place-id], [data-placeid]");

    const value =
      meta?.getAttribute("data-place-id") ||
      meta?.getAttribute("data-placeid");

    return value && ID.test(value) ? value : null;
  }

  function installGameBanner(client, win) {
    const doc = win.document;
    const bannerId = "webblox-play-game-banner";
    let lastUrl = win.location.href;

    function updateBanner() {
      const placeId = getCurrentPlaceId(win, client);
      const existing = doc.getElementById(bannerId);

      if (!placeId) {
        existing?.remove();
        return;
      }

      if (existing?.dataset.placeId === placeId) return;

      existing?.remove();

      if (!doc.body) return;

      const banner = doc.createElement("div");
      banner.id = bannerId;
      banner.dataset.placeId = placeId;

      Object.assign(banner.style, {
        position: "sticky",
        top: "0",
        zIndex: "2147483647",
        width: "100%",
        minHeight: "58px",
        boxSizing: "border-box",
        background: "#191b20",
        color: "#fff",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        gap: "16px",
        padding: "10px 20px",
        fontFamily: "Arial, sans-serif",
        borderBottom: "1px solid #34363d"
      });

      const label = doc.createElement("span");
      label.textContent = "Want to play this game?";

      Object.assign(label.style, {
        fontSize: "14px",
        fontWeight: "600"
      });

      const button = doc.createElement("button");
      button.type = "button";
      button.textContent = "Play this game";

      Object.assign(button.style, {
        background: "#fff",
        color: "#191b20",
        border: "0",
        borderRadius: "7px",
        padding: "10px 20px",
        fontSize: "14px",
        fontWeight: "700",
        cursor: "pointer"
      });

      button.addEventListener("click", () => openBlox({ placeId }));

      banner.append(label, button);
      doc.body.prepend(banner);
    }

    updateBanner();

    const observer = new win.MutationObserver(updateBanner);

    observer.observe(doc.documentElement, {
      childList: true,
      subtree: true
    });

    win.addEventListener("popstate", updateBanner);
    win.addEventListener("hashchange", updateBanner);

    win.setInterval(() => {
      if (win.location.href !== lastUrl) {
        lastUrl = win.location.href;
        updateBanner();
      }
    }, 500);
  }

  function popupFeatures() {
    const width = Math.round(screen.availWidth * 0.9);
    const height = Math.round(screen.availHeight * 0.9);
    const left = Math.round((screen.availWidth - width) / 2);
    const top = Math.round((screen.availHeight - height) / 2);

    return "popup=yes,width=" + width +
      ",height=" + height +
      ",left=" + left +
      ",top=" + top;
  }

  function open(url) {
    return window.open(url, "_blank", popupFeatures());
  }

  function notify(message) {
    window.alert(message);
  }

  function openBlox(launch) {
    if (!open(bloxUrl(launch))) {
      notify(MESSAGES["popup-blocked"]);
    }
  }

  async function lookupFollow(fetcher, follow) {
    const endpoint =
      "https://presence." + follow.domain + "/v1/presence/users";

    const body = JSON.stringify({
      userIds: [Number(follow.userId)]
    });

    const headers = {
      "content-type": "application/json"
    };

    let response;

    try {
      response = await fetcher(endpoint, {
        method: "POST",
        credentials: "include",
        headers,
        body
      });

      const csrf = response.status === 403
        ? response.headers.get("x-csrf-token")
        : null;

      if (csrf) {
        headers["x-csrf-token"] = csrf;

        response = await fetcher(endpoint, {
          method: "POST",
          credentials: "include",
          headers,
          body
        });
      }
    } catch {
      throw new Error("unreachable");
    }

    if (!response.ok) {
      throw new Error("unreachable");
    }

    const data = await response.json();

    const presence = Array.isArray(data.userPresences)
      ? data.userPresences[0]
      : null;

    if (!presence || presence.userPresenceType !== 2) {
      throw new Error("not-playing");
    }

    const placeId = String(presence.placeId || "");

    if (!ID.test(placeId)) {
      throw new Error("not-playing");
    }

    const gameId =
      typeof presence.gameId === "string" &&
      GAME_ID.test(presence.gameId)
        ? presence.gameId
        : null;

    return makeLaunch(placeId, gameId);
  }

  async function openFollow(fetcher, follow) {
    const popup = open("about:blank");

    if (!popup) {
      notify(MESSAGES["popup-blocked"]);
      return;
    }

    try {
      const launch = await lookupFollow(fetcher, follow);
      popup.location.replace(bloxUrl(launch));
    } catch (error) {
      popup.close();
      notify(MESSAGES[error.message] || MESSAGES.unreachable);
    }
  }

  function handleProtocol(win, raw) {
    if (typeof raw !== "string" || !PROTOCOL.test(raw)) {
      return false;
    }

    const launch = parseLaunchUri(raw);

    if (launch) {
      openBlox(launch);
      return true;
    }

    const follow = parseFollowUri(raw);

    if (follow) {
      void openFollow(win.fetch.bind(win), follow);
      return true;
    }

    notify(MESSAGES.unsupported);
    return true;
  }

  function resolveGamesStart(client, raw) {
    let absolute;

    try {
      absolute = new URL(raw, client.meta.base.href).href;
    } catch {
      return null;
    }

    const direct = parseGamesStart(absolute);

    if (direct) return direct;

    try {
      const original = client.unrewriteUrl(absolute);

      return original === absolute
        ? null
        : parseGamesStart(original);
    } catch {
      return null;
    }
  }

  function hookWindow({ client, window: win }) {
    const SELF_TARGETS = [
      "_self",
      "_parent",
      "_top",
      "_unfencedtop"
    ];

    client.Proxy("window.open", {
      apply(context) {
        const target = context.args[1] == null
          ? "_blank"
          : String(context.args[1]).toLowerCase();

        if (SELF_TARGETS.includes(target) && context.args[0] == null) {
          return;
        }

        const raw = context.args[0] == null
          ? ""
          : String(context.args[0]);

        if (handleProtocol(win, raw)) {
          context.return(null);
          return;
        }

        const launch = raw
          ? resolveGamesStart(client, raw)
          : null;

        if (!launch) return;

        context.return(null);
        openBlox(launch);
      }
    });

    const onClick = function(event) {
      if (
        event.defaultPrevented ||
        (event.button !== 0 && event.button !== 1)
      ) return;

      const anchor = event.composedPath().find(function(node) {
        return node instanceof win.Element &&
          node.matches("a[href], area[href]");
      });

      if (!anchor) return;

      const href = anchor.getAttribute("href") || "";

      if (handleProtocol(win, href)) {
        event.preventDefault();
        return;
      }

      const launch =
        resolveGamesStart(client, href) ||
        resolveGamesStart(client, anchor.href);

      if (!launch) return;

      event.preventDefault();
      openBlox(launch);
    };

    win.addEventListener("click", onClick);
    win.addEventListener("auxclick", onClick);

    client.RawTrap(win.HTMLIFrameElement.prototype, "src", {
      set(context, value) {
        if (!handleProtocol(win, value)) {
          context.set(value);
        }
      }
    });

    client.Proxy("Element.prototype.setAttribute", {
      apply(context) {
        const name = context.args[0];
        const value = context.args[1];

        if (
          typeof name !== "string" ||
          name.toLowerCase() !== "src" ||
          typeof value !== "string" ||
          !PROTOCOL.test(value) ||
          !(context.this instanceof win.HTMLIFrameElement)
        ) return;

        if (handleProtocol(win, value)) {
          context.return(undefined);
        }
      }
    });

    installGameBanner(client, win);
  }

  function interceptNavigation(context, result) {
    const parsed = context.parsed;

    if (parsed.isIframe) return;

    if (
      parsed.destination !== "document" &&
      parsed.destination !== "iframe"
    ) return;

    const launch = parseGamesStart(parsed.url.href);

    if (!launch) return;

    const Headers = context.request.initialHeaders.constructor;

    result.response = {
      body: "<!doctype html><meta charset='utf-8'><title>Launching Roblox</title><body style='margin:0;display:grid;place-items:center;height:100vh;font:15px system-ui;background:Canvas;color:CanvasText'>Opening Webblox…",
      headers: Headers.fromRawHeaders([
        ["content-type", "text/html; charset=utf-8"],
        ["cache-control", "no-store"]
      ]),
      status: 200,
      statusText: "OK"
    };

    openBlox(launch);
  }

  class BloxLaunchPlugin extends ManagedPlugin {
    constructor() {
      super("webblox-launch", []);
    }

    install(host) {
      super.install(host);

      this.tap(host.hooks.init.post, hookWindow);
      this.tap(host.hooks.fetch.intercept, interceptNavigation);
    }
  }

  return new BloxLaunchPlugin();
}
`;

function patchScramjetHTML(html) {
  const styleEnd = html.indexOf("</style>");

  if (styleEnd < 0) {
    throw new Error("Scramjet browser stylesheet was not found.");
  }

  const hideBrowserUI = `
#tabstrip,
#toolbar,
#bookmarks-bar,
#pz-new-tab,
#pz-home {
  display: none !important;
}
#pz-main {
  flex: 1 1 auto !important;
  width: 100%;
  height: 100%;
  min-height: 0;
}
body.bare #pz-main {
  height: 100vh;
}
`;

  html =
    html.slice(0, styleEnd) +
    hideBrowserUI +
    html.slice(styleEnd);

  const tabMarker = "function createTab(url) {";

  if (!html.includes(tabMarker)) {
    throw new Error("Could not locate Scramjet's tab creation function.");
  }

  html = html.replace(
    tabMarker,
    PLAY_PLUGIN + "\n" + tabMarker
  );

  const frameMarker = "frame: controller.createFrame(iframe),";

  if (!html.includes(frameMarker)) {
    throw new Error("Could not locate Scramjet's frame creation call.");
  }

  html = html.replace(
    frameMarker,
    `frame: controller.createFrame(iframe, {
      plugins: [
        createBloxLaunchPlugin(window._pnclg8f.ManagedPlugin)
      ]
    }),`
  );

  const defaultPage = "var first = pendingUrl || PROXY_HOME;";

  if (!html.includes(defaultPage)) {
    throw new Error("Could not locate Scramjet's initial page.");
  }

  html = html.replace(
    defaultPage,
    "var first = pendingUrl || 'https://www.roblox.com/';"
  );

  const bootMarker = "function boot() {\n  handleHashTarget();";

  if (!html.includes(bootMarker)) {
    throw new Error("Could not locate Scramjet's boot function.");
  }

  html = html.replace(
    bootMarker,
    "function boot() {\n  enterBareMode();\n  handleHashTarget();"
  );

  return html;
}

async function serveScramjetHome(request) {
  const upstream = await fetch(SCRAMJET_REPO + "/index.html", {
    headers: {
      Accept: "text/html"
    }
  });

  if (!upstream.ok) {
    return makeResponse(
      "Could not load Scramjet Browser: HTTP " + upstream.status,
      502,
      {
        "Content-Type": "text/plain; charset=utf-8"
      },
      request
    );
  }

  let html = await upstream.text();

  try {
    html = patchScramjetHTML(html);
  } catch (error) {
    return makeResponse(
      "Scramjet patch error:\n" + error.message,
      502,
      {
        "Content-Type": "text/plain; charset=utf-8"
      },
      request
    );
  }

  return makeResponse(
    html,
    200,
    {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
      "X-Robots-Tag": "noindex, nofollow"
    },
    request
  );
}

async function serveScramjetFile(path, request) {
  return proxy(
    SCRAMJET_REPO + "/" + path,
    request,
    {
      "Content-Type": getMime(path),
      "Cache-Control": "public, max-age=3600"
    }
  );
}

function patchWebbloxHost(source) {
  source = source.replaceAll(
    "__scramjet_controller",
    SCRAMJET_COOKIE_DB
  );

  const cookieStart = "function sessionFromDump(dump) {";
  const cookieEnd = "\n}\n\n// Puts a candidate session";
  const cs = source.indexOf(cookieStart);
  const ce = source.indexOf(cookieEnd, cs);

  if (cs < 0 || ce < 0) {
    throw new Error("Could not locate Webblox sessionFromDump().");
  }

  const sessionFromDump = `function sessionFromDump(dump) {
  if (typeof dump !== "string") return null;

  let cookies;

  try {
    cookies = JSON.parse(dump);
  } catch {
    return null;
  }

  const now = Date.now();

  const candidates = Object.values(cookies ?? {}).filter(cookie => {
    if (
      cookie?.name !== SESSION ||
      typeof cookie.value !== "string" ||
      !cookie.value
    ) return false;

    const domain = String(cookie.domain || "")
      .replace(/^\\./, "")
      .toLowerCase();

    if (
      domain !== "roblox.com" &&
      !domain.endsWith(".roblox.com")
    ) return false;

    if (cookie.expires) {
      const expiry = typeof cookie.expires === "number"
        ? cookie.expires
        : Date.parse(cookie.expires);

      if (Number.isFinite(expiry) && expiry <= now) return false;
    }

    return true;
  });

  candidates.sort((a, b) => {
    const rank = cookie => {
      const domain = String(cookie.domain || "")
        .replace(/^\\./, "")
        .toLowerCase();

      return domain === "roblox.com" ? 0 : domain.split(".").length;
    };

    return rank(a) - rank(b);
  });

  return candidates[0]?.value ?? null;
}`;

  source =
    source.slice(0, cs) +
    sessionFromDump +
    source.slice(ce + 2);

  const ensureStart = "async function ensureSession() {";
  const ensureEnd = "\n}\n\n// ---- Launcher";
  const es = source.indexOf(ensureStart);
  const ee = source.indexOf(ensureEnd, es);

  if (es < 0 || ee < 0) {
    throw new Error("Could not locate Webblox ensureSession().");
  }

  const ensureSession = `async function ensureSession() {
  const saved = savedSession();
  const manual = localStorage.getItem(MANUAL_KEY) === "1";

  const proxied = await proxySession();

  if (manual && saved) {
    try {
      if (await trySession(saved, true)) return;
    } catch (error) {
      console.warn("[blox] Saved manual session validation failed:", error);
    }
  }

  for (let attempt = 0; attempt < 10; attempt++) {
    const latest = attempt === 0 ? proxied : await proxySession();

    if (latest) {
      try {
        if (await trySession(latest, false)) return;
      } catch (error) {
        console.warn("[blox] Roblox session validation failed:", error);
      }
    }

    await new Promise(resolve => setTimeout(resolve, 250));
  }

  if (saved && !manual) {
    try {
      if (await trySession(saved, false)) return;
    } catch (error) {
      console.warn("[blox] Previous session validation failed:", error);
    }
  }

  return askForSession(saved !== null);
}`;

  source =
    source.slice(0, es) +
    ensureSession +
    source.slice(ee + 2);

  const launcherMarker = "// ---- Launcher";
  const launcherIndex = source.indexOf(launcherMarker);

  if (launcherIndex < 0) {
    throw new Error("Could not locate Webblox launcher section.");
  }

  const watcher = `function startSessionWatcher() {
  let current = savedSession();
  let checking = false;

  setInterval(async () => {
    if (checking) return;

    checking = true;

    try {
      const latest = await proxySession();

      if (!latest || latest === current) return;

      const previous = savedSession();
      const previousManual = localStorage.getItem(MANUAL_KEY) === "1";

      storeSession(latest, false);

      let accepted = false;

      try {
        accepted = await sessionAccepted();
      } catch (error) {
        console.warn("[blox] Could not validate switched account:", error);
      }

      if (accepted) {
        current = latest;
        console.log("[blox] Roblox account session refreshed.");
      } else if (previous) {
        storeSession(previous, previousManual);
      } else {
        forgetSession();
      }
    } catch (error) {
      console.warn("[blox] Account watcher error:", error);
    } finally {
      checking = false;
    }
  }, 1500);
}

`;

  source =
    source.slice(0, launcherIndex) +
    watcher +
    source.slice(launcherIndex);

  const bootCall = "await ensureSession();";

  if (!source.includes(bootCall)) {
    throw new Error("Could not locate Webblox boot session call.");
  }

  source = source.replace(
    bootCall,
    "await ensureSession();\n  startSessionWatcher();"
  );

  return source;
}

async function serveWebbloxHost(request) {
  const upstream = await fetch(WEBBLOX_REPO + "/host.js");

  if (!upstream.ok) {
    return makeResponse(
      "Could not load Webblox host.js.",
      502,
      {
        "Content-Type": "text/plain; charset=utf-8"
      },
      request,
      true
    );
  }

  let source = await upstream.text();

  try {
    source = patchWebbloxHost(source);
  } catch (error) {
    return makeResponse(
      "Webblox account patch error:\n" + error.message,
      502,
      {
        "Content-Type": "text/plain; charset=utf-8"
      },
      request,
      true
    );
  }

  return makeResponse(
    source,
    200,
    {
      "Content-Type": "application/javascript; charset=utf-8",
      "Cache-Control": "no-cache"
    },
    request,
    true
  );
}

async function getEngineVersion() {
  if (
    cachedEngineVersion &&
    Date.now() < engineVersionExpiry
  ) {
    return cachedEngineVersion;
  }

  const res = await fetch(ROBLOX_VERSION_URL, {
    headers: {
      Accept: "application/json"
    }
  });

  if (!res.ok) {
    throw new Error("Roblox version lookup failed: HTTP " + res.status);
  }

  const data = await res.json();

  cachedEngineVersion =
    data.clientVersionUpload ||
    data.version;

  if (!cachedEngineVersion) {
    throw new Error("Roblox did not return an engine version.");
  }

  engineVersionExpiry = Date.now() + 3600000;

  return cachedEngineVersion;
}

function engineCandidates(version, filename) {
  const safe = filename
    .split("/")
    .map(encodeURIComponent)
    .join("/");

  return [
    "https://setup.rbxcdn.com/webclient/" +
      version + "-RobloxWebClient/" + safe,

    "https://setup.rbxcdn.com/webclient/" +
      version + "/" + safe,

    "https://setup.rbxcdn.com/" +
      version + "-RobloxWebClient/" + safe
  ];
}

async function serveEngineFile(filename, request) {
  if (
    !filename ||
    filename.includes("..") ||
    filename.startsWith("/")
  ) {
    return makeResponse(
      "Invalid engine path.",
      400,
      {
        "Content-Type": "text/plain; charset=utf-8"
      },
      request,
      true
    );
  }

  let version;

  try {
    version = await getEngineVersion();
  } catch (error) {
    return makeResponse(
      error.message,
      502,
      {
        "Content-Type": "text/plain; charset=utf-8"
      },
      request,
      true
    );
  }

  for (const candidate of engineCandidates(version, filename)) {
    try {
      const result = await proxy(
        candidate,
        request,
        {
          "Content-Type": getMime(filename),
          "Cache-Control": "public, max-age=3600"
        },
        true
      );

      if (result.ok || result.status === 206) {
        return result;
      }

      try {
        await result.body?.cancel();
      } catch {}
    } catch {}
  }

  return makeResponse(
    "Roblox engine file not found: " + filename +
      "\nEngine version: " + version,
    404,
    {
      "Content-Type": "text/plain; charset=utf-8"
    },
    request,
    true
  );
}

async function engineInfo(request) {
  try {
    const version = await getEngineVersion();
    const candidates = engineCandidates(version, "WebClient.js");
    const results = [];

    for (const url of candidates) {
      try {
        const res = await fetch(url, {
          method: "HEAD"
        });

        results.push({
          url,
          status: res.status,
          available: res.ok
        });
      } catch (error) {
        results.push({
          url,
          available: false,
          error: error.message
        });
      }
    }

    return makeResponse(
      JSON.stringify({
        success: true,
        version,
        candidates: results
      }, null, 2),
      200,
      {
        "Content-Type": "application/json; charset=utf-8"
      },
      request,
      true
    );
  } catch (error) {
    return makeResponse(
      JSON.stringify({
        success: false,
        error: error.message
      }, null, 2),
      502,
      {
        "Content-Type": "application/json; charset=utf-8"
      },
      request,
      true
    );
  }
}

async function serveWebbloxFile(path, request, type) {
  return proxy(
    WEBBLOX_REPO + "/" + path,
    request,
    {
      "Content-Type": type || getMime(path),
      "Cache-Control": path === "index.html"
        ? "no-cache"
        : "public, max-age=3600"
    },
    true
  );
}

function redirect(url) {
  return new Response(null, {
    status: 302,
    headers: makeHeaders({
      Location: url
    })
  });
}

export default {
  async fetch(request, env, ctx) {
    try {
      if (
        request.method !== "GET" &&
        request.method !== "HEAD"
      ) {
        return makeResponse(
          "Method not allowed.",
          405,
          {
            Allow: "GET, HEAD",
            "Content-Type": "text/plain; charset=utf-8"
          },
          request
        );
      }

      const url = new URL(request.url);

      let pathname;

      try {
        pathname = decodeURIComponent(url.pathname);
      } catch {
        return makeResponse(
          "Invalid URL path.",
          400,
          {},
          request
        );
      }

      if (pathname === "/wisp") {
        return redirect(new URL("/wisp/", url.origin).href);
      }

      if (pathname === "/wisp/") {
        return serveWisp(request);
      }

      if (
        pathname === "/" ||
        pathname === "/index.html"
      ) {
        return serveScramjetHome(request);
      }

      if (
        pathname === "/x7q2.js" ||
        pathname === "/sw.js"
      ) {
        return proxy(
          SCRAMJET_REPO + pathname,
          request,
          {
            "Content-Type": "application/javascript; charset=utf-8",
            "Service-Worker-Allowed": "/",
            "Cache-Control": "no-cache"
          }
        );
      }

      if (pathname.startsWith("/assets/prox/")) {
        const file = pathname.slice("/assets/prox/".length);

        return serveScramjetFile(
          "assets/prox/" + file,
          request
        );
      }

      if (
        pathname === "/app.svg" ||
        pathname === "/index.svg"
      ) {
        return serveScramjetFile(
          pathname.slice(1),
          request
        );
      }

      if (pathname.startsWith("/gh/")) {
        const file = pathname.slice("/gh/".length);

        if (!file || file.includes("..")) {
          return makeResponse(
            "Invalid CDN path.",
            400,
            {},
            request
          );
        }

        return proxy(
          "https://cdn.jsdelivr.net/gh/" + file + url.search,
          request,
          {
            "Content-Type": getMime(file),
            "Cache-Control": "public, max-age=3600"
          }
        );
      }

      if (pathname === "/blox") {
        const destination = new URL("/blox/", url.origin);
        destination.search = url.search;

        return redirect(destination.href);
      }

      if (pathname === "/blox/") {
        if (!url.searchParams.has("wisp")) {
          const destination = new URL(request.url);
          const wisp = new URL(DEFAULT_WISP_PATH, url.origin);

          wisp.protocol = url.protocol === "https:"
            ? "wss:"
            : "ws:";

          destination.searchParams.set("wisp", wisp.href);

          return redirect(destination.href);
        }

        return serveWebbloxFile(
          "index.html",
          request,
          "text/html; charset=utf-8"
        );
      }

      if (pathname === "/blox/index.html") {
        return serveWebbloxFile(
          "index.html",
          request,
          "text/html; charset=utf-8"
        );
      }

      if (pathname === "/blox/host.js") {
        return serveWebbloxHost(request);
      }

      if (pathname === "/blox/host.css") {
        return serveWebbloxFile(
          "host.css",
          request,
          "text/css; charset=utf-8"
        );
      }

      if (pathname === "/blox/sw.js") {
        return serveWebbloxFile(
          "sw.js",
          request,
          "application/javascript; charset=utf-8"
        );
      }

      if (pathname === "/blox/epoxy.mjs") {
        return proxy(
          EPOXY_URL,
          request,
          {
            "Content-Type": "application/javascript; charset=utf-8",
            "Cache-Control": "public, max-age=3600"
          },
          true
        );
      }

      if (pathname.startsWith("/blox/engine/")) {
        return serveEngineFile(
          pathname.slice("/blox/engine/".length),
          request
        );
      }

      if (pathname === "/__engine_info") {
        return engineInfo(request);
      }

      return makeResponse(
        "Not found: " + pathname,
        404,
        {
          "Content-Type": "text/plain; charset=utf-8"
        },
        request
      );
    } catch (error) {
      return makeResponse(
        "Worker error:\n" +
          (error.stack || error.message || String(error)),
        500,
        {
          "Content-Type": "text/plain; charset=utf-8"
        },
        request
      );
    }
  }
};
