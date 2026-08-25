# website/

This directory builds the site. It holds no documentation content.

All prose lives in `docs/` at the repository root, as plain markdown that
renders on GitHub. `pnpm run docs:project` copies it into `.generated/`,
which is gitignored, never edited, and never committed.

If you are about to add a `.md` file here, you want `docs/` instead.
