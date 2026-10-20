import fs from "node:fs";
import path from "node:path";
import YAML from "yaml";

const FM_RE = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/;

export function listMarkdown(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...listMarkdown(p));
    else if (entry.name.endsWith(".md")) out.push(p);
  }
  return out;
}

export function readPost(file) {
  const raw = fs.readFileSync(file, "utf8");
  const m = raw.match(FM_RE);
  if (!m) return { data: {}, body: raw };
  return { data: YAML.parse(m[1]) ?? {}, body: raw.slice(m[0].length) };
}

export function writePost(file, data, body) {
  const fm = YAML.stringify(data, { lineWidth: 0 });
  fs.writeFileSync(file, `---\n${fm}---\n${body}`);
}

export function asList(value) {
  if (value == null) return [];
  return Array.isArray(value) ? value : [value];
}
