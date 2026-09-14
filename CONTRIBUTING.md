# Contributing to ha-eufy-sdk-bridge

Thanks for helping out! This guide covers **how we branch and merge** so your PR lands smoothly.

## Branch model — target `dev`, not `main`

We use two long-lived branches:

- **`dev`** — the integration branch. **All contributions go here.**
- **`main`** — release-only. It's what gets tagged and published as a Docker image.

```
your branch  ──►  PR into `dev`  ──►  maintainer review + green CI  ──►  merged to dev
                                                                            │
                              (when a release is ready) maintainers open    ▼
                              a  dev ─► main  PR, merge it, then tag ──►  image published
```

So the flow for a contribution is:

1. Fork (or, if you're a maintainer, branch the repo).
2. **Create your branch from `dev`.**
3. Make your change, keep it focused, update docs if behaviour changes.
4. **Open your pull request against `dev`.** PRs opened against `main` will be asked to retarget.
5. A maintainer reviews and merges.

> **Please don't open PRs into `main`.** `main` moves only when the maintainers cut a release by
> merging `dev → main` and tagging it. Both `main` and `dev` are protected — everything lands via PR.

## Who can merge

- **Maintainers** (repo owners) can merge PRs and cut releases.
- **Everyone else**: your PR needs an approving review from a maintainer before it can merge. CI must
  be green.

## Before you push — run the same checks CI does

The PR gate (`.github/workflows/ci.yml`) runs on every PR into `main`/`dev` and must pass:

```bash
npm ci            # install exactly what the lockfile pins
npm run lint      # Prettier — formatting is enforced, not debated
npm test          # test suite
# syntax-check every ESM module (this is an ESM project, no tsc):
for f in $(git ls-files '*.mjs'); do node --check "$f"; done
```

Auto-fix formatting before committing:

```bash
npm run format
```

## Docker images (FYI)

You don't build or publish images in a PR — CI does it automatically:

- Merging to **`dev`** publishes `ghcr.io/razzledazzlebazzle/ha-eufy-sdk-bridge:dev`.
- Publishing a **GitHub Release** (off `main`) builds and pushes the versioned + `:latest` tags.

## Reporting bugs

Open an [issue](../../issues/new/choose) with a clear summary, steps to reproduce, what you expected
vs. what happened, and relevant logs (run the bridge with `BRIDGE_DEBUG=1` for a control trace).

## License

By contributing, you agree that your contributions are licensed under the project's
[Apache License 2.0](./LICENSE).
