// Second figbridge-mcp on the same port attaches to the first bridge instead
// of grabbing 7332+ (keeps Cursor/Codex on FIGBRIDGE_PORT aligned with plugin).
//
// Run: node test/attach-bridge.mjs

import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const bridgePath = pathToFileURL(path.join(__dirname, "..", "mcp", "src", "bridge.js")).href;
const { startBridge, probeFigbridgeListening, createAttachedBridgeClient } = await import(bridgePath);

const PORT = 7359;

function noop() {}

async function main() {
  const a = await startBridge(PORT, noop, 0);
  if (a.attached !== false || !a.server) throw new Error("expected primary bridge listener");
  try {
    if (!(await probeFigbridgeListening(PORT))) throw new Error("probe failed");

    const b = await startBridge(PORT, noop, 0);
    if (!b.attached || b.server) throw new Error("expected attach without new listener");

    const client = createAttachedBridgeClient(PORT);
    try {
      await client.sendCommand("noop", {}, 500);
      throw new Error("expected command error without plugin");
    } catch (e) {
      if (!(e.message && e.message.includes("Figbridge"))) {
        throw new Error(`unexpected error: ${e.message}`);
      }
    }
    process.stderr.write("[attach-bridge.mjs] ok\n");
  } finally {
    await new Promise((resolve) => a.server.close(() => resolve()));
  }
}

main().catch((e) => {
  console.error("[attach-bridge.mjs]", e.stack || e);
  process.exit(1);
});
