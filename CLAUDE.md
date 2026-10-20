# Blog content

Posts are Markdown files in `content/posts/`, built with Hugo (DoIt theme). Images live in `static/images/`.

## Tags and categories

- Before adding a tag or category, run `npm run tags` and reuse an existing term when one fits.
- Every term must be listed in `data/taxonomy.yaml`; CI runs `npm run tags:check` and fails otherwise. Add a genuinely new term there in the same change.
- Tags are specific technologies or topics (e.g. `Trino`, `Iceberg`). Categories are broad areas; keep them to a handful.
- Match the casing of existing terms exactly.
