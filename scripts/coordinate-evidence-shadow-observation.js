#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import path from "node:path";
import { execFileSync } from "node:child_process";
import {
  SHADOW_OBSERVATION_CATEGORY,
  buildCoordinateEvidenceShadowObservation
} from "../server/coordinate-evidence/index.js";

function parseArgs(argv = []) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const item = argv[index];
    if (!item.startsWith("--")) continue;
    const key = item.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      args[key] = true;
    } else {
      args[key] = next;
      index += 1;
    }
  }
  return args;
}

function usage() {
  return [
    "Usage:",
    "  node scripts/coordinate-evidence-shadow-observation.js --input <debug-response.json> --category <category> --sample-id <id>",
    "",
    "Categories:",
    `  ${Object.values(SHADOW_OBSERVATION_CATEGORY).join(", ")}`,
    "",
    "This harness emits sanitized coordinate_evidence_shadow_observation_v1 JSON only."
  ].join("\n");
}

function gitValue(args = []) {
  try {
    return execFileSync("git", args, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    return "";
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.input) {
    console.log(usage());
    process.exitCode = args.help ? 0 : 1;
    return;
  }

  const inputPath = path.resolve(String(args.input));
  const response = JSON.parse((await readFile(inputPath, "utf8")).replace(/^\uFEFF/, ""));
  const observation = buildCoordinateEvidenceShadowObservation({
    response,
    sampleId: args["sample-id"] || args.sampleId || path.basename(inputPath, path.extname(inputPath)),
    timestamp: args.timestamp,
    commit: args.commit || gitValue(["rev-parse", "HEAD"]),
    branch: args.branch || gitValue(["branch", "--show-current"]),
    category: args.category || SHADOW_OBSERVATION_CATEGORY.PENDING_FIXTURE,
    fixture: {
      fileName: args["fixture-file"] || args.fixtureFile || path.basename(inputPath),
      fixtureStatus: args["fixture-status"] || args.fixtureStatus || "real_current_capture",
      fixtureHash: args["fixture-hash"] || args.fixtureHash || null
    }
  });

  console.log(JSON.stringify(observation, null, 2));
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
