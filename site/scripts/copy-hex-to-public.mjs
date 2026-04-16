/**
 * Prebuild/predev script: copies grid*.json (hex + per-window grids) from
 * src/data/ to public/data/ so they are accessible at /data/* at runtime.
 * Only copies files that exist — missing files are handled gracefully by the UI.
 */
import { copyFileSync, existsSync, mkdirSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const srcDataDir = join(__dirname, "../src/data");
const publicDataDir = join(__dirname, "../public/data");

if (!existsSync(publicDataDir)) {
  mkdirSync(publicDataDir, { recursive: true });
}

// Match grid_hex_r*.json and grid_*.json (per-window grid files)
const files = readdirSync(srcDataDir).filter(
  (f) =>
    /^grid_hex_r\d+(_[a-z_0-9]+)?\.json$/.test(f) ||
    /^grid_[a-z_0-9]+\.json$/.test(f),
);

for (const file of files) {
  copyFileSync(join(srcDataDir, file), join(publicDataDir, file));
  console.log(`[copy-data] ${file} → public/data/`);
}

if (files.length === 0) {
  console.log("[copy-data] No grid*.json files found in src/data/ — skipping.");
}
