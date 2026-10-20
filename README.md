# zivali.github.io

Ziva Li's blog, built with [Hugo](https://gohugo.io/) and the DoIt theme, deployed to GitHub Pages by `.github/workflows/cd.yml` on every push to `main`.

Originally bootstrapped from [Notion-Hugo](https://github.com/HEIGE-PCloud/Notion-Hugo); posts now live in this repo as Markdown.

## Writing

- Posts: `content/posts/<slug>.md`. URLs come from the title (`config/_default/permalinks.toml`), not the filename.
- Images: `static/images/`, referenced as `/images/...`. Other files (PDF, video): `static/files/`.
- Preview locally: `hugo server`

## Tags and categories

Every tag and category must be listed in `data/taxonomy.yaml`; CI fails otherwise.

```sh
npm install
npm run tags        # list terms in use with post counts
npm run tags:check  # what CI runs
```

## License

This project is open sourced under the [GNU GPL license v3](https://www.gnu.org/licenses/gpl-3.0.html).
