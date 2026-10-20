// One-off migration: strip Notion-only front matter and drop Notion IDs from filenames.
// Permalinks are built from :title, so renaming files does not change URLs.
import fs from "node:fs";
import path from "node:path";
import { listMarkdown, readPost, writePost } from "./frontmatter.mjs";

const NOTION_FIELDS = ["NOTION_METADATA", "UPDATE_TIME", "EXPIRY_TIME"];
const NOTION_ID_SUFFIX = /-?[0-9a-f]{32}$/;

function slugify(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9㐀-鿿]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

const dryRun = process.argv.includes("--dry-run");
const taken = new Set();

for (const file of listMarkdown("content")) {
  const { data, body } = readPost(file);
  const notionId = data.NOTION_METADATA?.id?.replace(/-/g, "") ?? "";
  for (const field of NOTION_FIELDS) delete data[field];

  const base = path.basename(file, ".md");
  let slug = slugify(base.replace(NOTION_ID_SUFFIX, ""));
  if (!slug) slug = `untitled-${notionId.slice(0, 8) || "post"}`;
  let target = path.join(path.dirname(file), `${slug}.md`);
  for (let i = 2; taken.has(target); i++) {
    target = path.join(path.dirname(file), `${slug}-${i}.md`);
  }
  taken.add(target);

  console.log(`${file}\n  -> ${target}`);
  if (dryRun) continue;
  writePost(file, data, body);
  if (target !== file) fs.renameSync(file, target);
}
