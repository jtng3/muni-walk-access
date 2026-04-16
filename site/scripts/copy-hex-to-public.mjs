/**
 * Prebuild/predev script: copies grid_hex_r*.json from src/data/ to public/data/
 * so they are accessible at /data/grid_hex_r{N}.json at runtime.
 * Only copies files that exist — missing resolutions are handled gracefully by
 * the resolution picker (fetch 404 → disable that option).
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

const files = readdirSync(srcDataDir).filter((f) =>
  /^grid_hex_r\d+\.json$/.test(f),
);

for (const file of files) {
  copyFileSync(join(srcDataDir, file), join(publicDataDir, file));
  console.log(`[copy-hex] ${file} → public/data/`);
}

if (files.length === 0) {
  console.log(
    "[copy-hex] No grid_hex_r*.json files found in src/data/ — skipping.",
  );
}
