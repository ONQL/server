# Releasing ONQL Server

Releases are fully automated. GoReleaser builds binaries for every supported OS/arch and uploads them to the GitHub Releases page when a version tag is pushed.

## Cut a release

```bash
git tag v0.1.0
git push origin v0.1.0
```

That's it. The workflow in `.github/workflows/release.yml` runs on the tag push and invokes GoReleaser, which:

1. Builds `onql-server` for Linux (amd64, arm64, 386, arm), Windows (amd64, 386), macOS (amd64, arm64), and FreeBSD (amd64, arm64).
2. Packages each binary into a `.tar.gz` (or `.zip` on Windows) with README + LICENSE.
3. Produces Linux `.deb`, `.rpm`, and `.apk` packages via `nfpm`.
4. Generates `checksums.txt` with SHA-256 sums.
5. Creates a GitHub Release at `github.com/ONQL/server/releases/tag/v0.1.0` with auto-generated changelog and uploads everything.

## Output formats

| Platform | Architectures | Formats |
|---|---|---|
| Linux | amd64, arm64, 386, arm/v7 | `.tar.gz`, `.deb`, `.rpm`, `.apk` |
| Windows | amd64, 386 | `.zip` |
| macOS | amd64, arm64 | `.tar.gz` |
| FreeBSD | amd64, arm64 | `.tar.gz` |

MSI for Windows and DMG for macOS will be added in a later release.

## Versioning

Use [semver](https://semver.org): `v<MAJOR>.<MINOR>.<PATCH>`. Pre-releases use suffixes (`v0.2.0-rc.1`), which GoReleaser auto-marks as prereleases.

## Local dry-run

```bash
goreleaser release --snapshot --clean --skip=publish
```

Builds everything under `./dist/` without touching GitHub. Useful to verify config changes before tagging.

## Requirements on the runner

The GitHub Actions workflow uses `ubuntu-latest` with Go 1.21. Cross-compilation is handled by the Go toolchain — no CGO, no extra toolchains needed.

## Troubleshooting

- **"tag already exists"** — delete the tag locally and on remote, then re-push: `git tag -d v0.1.0 && git push origin :refs/tags/v0.1.0`.
- **Build fails on one arch** — check `ignore:` block in `.goreleaser.yml`; some OS/arch combos aren't supported.
- **Changelog is empty** — commits since last tag must match the regex filters (`feat:`, `fix:`, etc.) or fall through to "Others".
