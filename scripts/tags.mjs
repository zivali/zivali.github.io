// List tags/categories used in content/, or check them against data/taxonomy.yaml.
//   node scripts/tags.mjs          show every term and how many posts use it
//   node scripts/tags.mjs --check  fail if a post uses a term not in data/taxonomy.yaml
//   node scripts/tags.mjs --init   write data/taxonomy.yaml from the terms in use
import fs from "node:fs";
import YAML from "yaml";
import { asList, listMarkdown, readPost } from "./frontmatter.mjs";

const TAXONOMIES = ["tags", "categories"];
const ALLOWLIST = "data/taxonomy.yaml";

// taxonomy -> term -> files using it
const usage = Object.fromEntries(TAXONOMIES.map((t) => [t, new Map()]));
for (const file of listMarkdown("content")) {
  const { data } = readPost(file);
  for (const tax of TAXONOMIES) {
    for (const term of asList(data[tax])) {
      const files = usage[tax].get(term) ?? [];
      files.push(file);
      usage[tax].set(term, files);
    }
  }
}

const mode = process.argv[2];

if (mode === "--init") {
  const allow = Object.fromEntries(
    TAXONOMIES.map((t) => [t, [...usage[t].keys()].sort()])
  );
  fs.writeFileSync(ALLOWLIST, YAML.stringify(allow));
  console.log(`Wrote ${ALLOWLIST}`);
} else if (mode === "--check") {
  const allow = YAML.parse(fs.readFileSync(ALLOWLIST, "utf8")) ?? {};
  let failed = false;
  for (const tax of TAXONOMIES) {
    const allowed = new Set(asList(allow[tax]));
    for (const [term, files] of usage[tax]) {
      if (allowed.has(term)) continue;
      failed = true;
      const similar = [...allowed].find(
        (a) => a.toLowerCase() === String(term).toLowerCase()
      );
      const hint = similar ? ` (did you mean "${similar}"?)` : "";
      console.error(`Unknown ${tax} "${term}"${hint} in:`);
      for (const f of files) console.error(`  ${f}`);
    }
  }
  if (failed) {
    console.error(`\nUse an existing term, or add the new one to ${ALLOWLIST}.`);
    process.exit(1);
  }
  console.log("All tags and categories are in the allowlist.");
} else {
  for (const tax of TAXONOMIES) {
    console.log(`${tax}:`);
    const sorted = [...usage[tax]].sort((a, b) => b[1].length - a[1].length);
    for (const [term, files] of sorted) console.log(`  ${files.length}  ${term}`);
    if (sorted.length === 0) console.log("  (none)");
  }
}
