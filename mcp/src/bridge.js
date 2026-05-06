import http from "node:http";
import crypto from "node:crypto";
import { setLatest, getLatest, getHistory, getHistorySince } from "./store.js";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age": "86400"
};

function send(res, status, body, type = "application/json") {
  res.writeHead(status, { "Content-Type": type, ...CORS_HEADERS });
  res.end(typeof body === "string" ? body : JSON.stringify(body));
}

// ── SSE registry ─────────────────────────────────────────────
const clients = new Set();

function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of clients) {
    try { res.write(payload); } catch {}
  }
}

// ── Command queue (agent → plugin round-trip) ────────────────
const pending = new Map(); // cmdId → { resolve, reject, timer }

export function sendCommand(action, args, timeoutMs = 5000) {
  if (clients.size === 0) {
    return Promise.reject(new Error("Figbridge plugin is not connected. Open the plugin in Figma and toggle Live bridge on."));
  }
  const cmdId = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(cmdId);
      reject(new Error(`Command "${action}" timed out after ${timeoutMs}ms. Is the plugin still open?`));
    }, timeoutMs);
    pending.set(cmdId, { resolve, reject, timer });
    broadcast("command", { cmdId, action, args });
  });
}

export function clientCount() { return clients.size; }

// ── HTTP server ──────────────────────────────────────────────
// Try the requested port first; on EADDRINUSE walk up to `portRange`
// additional ports. Claude Desktop can respawn this process while an
// older instance is still holding 7331, so a hard fatal there means
// the user sees "Server disconnected" with no useful recovery. With
// fallback, the new instance just picks 7332 and the plugin (which
// probes the range) finds it. Resolves to `{ server, port, attached?: true }`.

const BRIDGE_HEALTH_NAME = "figbridge-bridge";

export async function probeFigbridgeListening(port, host = "127.0.0.1") {
  try {
    const r = await fetch(`http://${host}:${port}/health`);
    if (!r.ok) return false;
    const j = await r.json();
    return j && j.ok === true && j.name === BRIDGE_HEALTH_NAME;
  } catch {
    return false;
  }
}

/** HTTP delegate for MCP processes attached to another instance's bridge. */
export function createAttachedBridgeClient(port, _log = () => {}, host = "127.0.0.1") {
  const base = `http://${host}:${port}`;
  async function postCommand(action, args, timeoutMs = 5000) {
    const ac = globalThis.AbortSignal && typeof AbortSignal.timeout === "function"
      ? AbortSignal.timeout(timeoutMs + 2000)
      : undefined;
    const r = await fetch(`${base}/agent/command`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, args, timeoutMs }),
      signal: ac
    });
    if (!r.ok) throw new Error(`bridge returned HTTP ${r.status}`);
    const j = await r.json();
    if (!j.ok) throw new Error(j.error || "agent command failed");
    return j.result;
  }
  async function fetchClientCount() {
    try {
      const r = await fetch(`${base}/health`);
      if (!r.ok) return 0;
      const j = await r.json();
      return typeof j.clients === "number" ? j.clients : 0;
    } catch {
      return 0;
    }
  }
  return {
    sendCommand: postCommand,
    fetchClientCount,
    bridgeBaseUrl: base
  };
}

export function startBridge(preferredPort = 7331, log = () => {}, portRange = 9) {
  const server = http.createServer((req, res) => {
    if (req.method === "OPTIONS") { res.writeHead(204, CORS_HEADERS); return res.end(); }

    if (req.method === "GET" && req.url === "/health") {
      return send(res, 200, {
        ok: true, name: "figbridge-bridge",
        hasLatest: !!getLatest(), clients: clients.size,
        rpc: ["agent/command"]
      });
    }

    if (req.method === "GET" && req.url === "/latest") {
      return send(res, 200, getLatest() || { empty: true });
    }

    if (req.method === "GET" && req.url === "/history") {
      return send(res, 200, { history: getHistory() });
    }

    if (req.method === "GET" && req.url.startsWith("/history-since")) {
      const qs = req.url.includes("?") ? new URLSearchParams(req.url.slice(req.url.indexOf("?"))) : new URLSearchParams();
      const sinceMs = qs.get("since") || qs.get("sinceMs") || "0";
      return send(res, 200, { entries: getHistorySince(Number(sinceMs) || 0) });
    }

    // MCP sibling process forwards plugin commands via this route (canonical bridge owns the plugin SSE socket).
    if (req.method === "POST" && req.url === "/agent/command") {
      let chunks = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", async () => {
        try {
          const body = JSON.parse(Buffer.concat(chunks).toString() || "{}");
          const { action, args, timeoutMs } = body;
          if (!action) return send(res, 400, { ok: false, error: "missing action" });
          const tout = typeof timeoutMs === "number" && timeoutMs >= 1000 ? timeoutMs : 5000;
          const result = await sendCommand(action, args || {}, tout);
          return send(res, 200, { ok: true, result });
        } catch (e) {
          return send(res, 200, { ok: false, error: e && e.message ? e.message : String(e) });
        }
      });
      req.on("error", () => {});
      return;
    }

    // SSE — plugin subscribes here
    if (req.method === "GET" && req.url === "/events") {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        ...CORS_HEADERS
      });
      res.write(`event: hello\ndata: ${JSON.stringify({ ok: true, serverTime: Date.now() })}\n\n`);
      clients.add(res);
      log(`client connected (total=${clients.size})`);
      const keepalive = setInterval(() => { try { res.write(": ping\n\n"); } catch {} }, 20000);
      req.on("close", () => {
        clients.delete(res);
        clearInterval(keepalive);
        log(`client disconnected (total=${clients.size})`);
      });
      return;
    }

    // Plugin push a new selection payload
    if (req.method === "POST" && req.url === "/push") {
      let chunks = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        try {
          const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          setLatest(body);
          log(`push: ${body.pageName || "?"} / ${(body.nodeNames || []).join(", ")}`);
          broadcast("selection", {
            pageName: body.pageName, nodeNames: body.nodeNames,
            nodeIds: body.nodeIds, capturedAt: body.capturedAt
          });
          send(res, 200, { ok: true });
        } catch (e) { send(res, 400, { ok: false, error: e.message }); }
      });
      req.on("error", (e) => send(res, 500, { ok: false, error: e.message }));
      return;
    }

    // Plugin reports a command result
    const m = req.url && req.url.match(/^\/command\/([^/]+)\/result$/);
    if (req.method === "POST" && m) {
      const cmdId = m[1];
      let chunks = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        try {
          const body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
          const p = pending.get(cmdId);
          if (!p) return send(res, 404, { ok: false, error: "unknown cmdId" });
          clearTimeout(p.timer);
          pending.delete(cmdId);
          if (body.ok === false) p.reject(new Error(body.error || "command failed"));
          else p.resolve(body);
          send(res, 200, { ok: true });
        } catch (e) { send(res, 400, { ok: false, error: e.message }); }
      });
      return;
    }

    send(res, 404, { error: "not found" });
  });

  return new Promise((resolve, reject) => {
    let attempt = 0;
    let settled = false;
    const onError = (e) => {
      if (settled) return;
      if (e && e.code === "EADDRINUSE") {
        const triedPort = preferredPort + attempt;
        void (async () => {
          if (settled) return;
          if (await probeFigbridgeListening(triedPort)) {
            settled = true;
            server.removeListener("error", onError);
            try { server.close(); } catch {}
            log(`port ${triedPort} already has figbridge; attaching MCP to it (HTTP commands only)`);
            resolve({ server: null, port: triedPort, attached: true });
            return;
          }
          if (settled) return;
          if (attempt < portRange) {
            attempt++;
            const next = preferredPort + attempt;
            log(`port ${triedPort} in use, trying ${next}`);
            setImmediate(() => { if (!settled) server.listen(next, "127.0.0.1"); });
            return;
          }
          settled = true;
          server.removeListener("error", onError);
          reject(e);
        })();
        return;
      }
      settled = true;
      server.removeListener("error", onError);
      reject(e);
    };
    server.on("error", onError);
    server.once("listening", () => {
      settled = true;
      server.removeListener("error", onError);
      const addr = server.address();
      const port = addr && typeof addr === "object" ? addr.port : preferredPort + attempt;
      log(`bridge listening on http://127.0.0.1:${port}`);
      resolve({ server, port, attached: false });
    });
    server.listen(preferredPort, "127.0.0.1");
  });
}
