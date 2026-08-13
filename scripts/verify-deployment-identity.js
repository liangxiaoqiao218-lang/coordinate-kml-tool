import fs from "node:fs";
import { verifyDeploymentIdentity } from "../server/release-identity/index.js";

function parseArgs(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--expected") {
      const next = argv[index + 1];
      if (!next) throw new Error("--expected requires a JSON file path.");
      options.expectedPath = next;
      index += 1;
    } else if (arg === "--runtime") {
      const next = argv[index + 1];
      if (!next) throw new Error("--runtime requires a JSON file path.");
      options.runtimePath = next;
      index += 1;
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  if (!options.expectedPath || !options.runtimePath) {
    throw new Error("Usage: node scripts/verify-deployment-identity.js --expected expected.json --runtime runtime.json");
  }
  return options;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  const result = verifyDeploymentIdentity(
    readJson(options.expectedPath),
    readJson(options.runtimePath)
  );
  console.log(JSON.stringify(result, null, 2));
  if (result.status !== "MATCH") {
    process.exitCode = 1;
  }
}

main();
