/**
 * Prebuild/predev script: copies *.seed.json fixtures into place when the
 * pipeline-generated equivalents are absent (fresh clone or CI with no prior
 * pipeline run).  Real pipeline output (gitignored) takes precedence when it
 * exists -- this script never overwrites an existing file.
 */
import { copyFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";

const dataDir = join(dirname(fileURLToPath(import.meta.url)), "../src/data");

const fixtures = [
  { seed: "grid.seed.json", target: "grid.json" },
  { seed: "config_snapshot.seed.json", target: "config_snapshot.json" },
  { seed: "validation_results.seed.json", target: "validation_results.json" },
];

for (const { seed, target } of fixtures) {
  const targetPath = join(dataDir, target);
  const seedPath = join(dataDir, seed);
  if (!existsSync(targetPath)) {
    copyFileSync(seedPath, targetPath);
    console.log(`[ensure-data] Seeded ${target} from ${seed}`);
  }
}
