# Releasing Apache Asyncband (Incubating)

This document is a runbook for release managers. Publishing software has legal consequences, so follow the current [ASF Release Policy](https://www.apache.org/legal/release-policy.html), [ASF Release Distribution Policy](https://infra.apache.org/release-distribution), and [Incubator release guidance](https://incubator.apache.org/guides/releasemanagement.html) if they differ from this document.

> [!IMPORTANT]
>
> The signed source archive approved by the Apache Incubator PMC is the official Apache release. The crates.io package is a convenience distribution built from the same approved commit; it must not be published before the Incubator vote passes.

The process deliberately separates release candidates from publication. A signed `vX.Y.Z-rc.N` tag identifies the exact source candidate and only performs a crates.io dry run. After both votes pass, a signed `vX.Y.Z` tag is added to the same commit and triggers the protected crates.io publishing job.

## Terminology

- `VERSION` is the proposed final version, for example `0.7.0`.
- `RC` is the positive release candidate number, for example `1`.
- `CANDIDATE` is `${VERSION}-rc.${RC}`, for example `0.7.0-rc.1`.
- `RC_TAG` is `v${CANDIDATE}` and `FINAL_TAG` is `v${VERSION}`.
- The official source archive is `apache-asyncband-${VERSION}-incubating-src.tar.gz`. The RC number belongs to the staging directory and tag, not the artifact filename.

## One-time setup

### GPG and ASF distribution directories

The release manager needs an ASF-associated GPG key whose public key is available from the project `KEYS` file. Follow the [ASF release signing guide](https://infra.apache.org/release-signing.html), publish the public key, and verify its fingerprint through an independent channel.

Before the first release, create the project directories if they do not exist:

```shell
svn mkdir --parents https://dist.apache.org/repos/dist/dev/incubator/asyncband \
  -m "Initialize Apache Asyncband development distribution area"
svn mkdir --parents https://dist.apache.org/repos/dist/release/incubator/asyncband \
  -m "Initialize Apache Asyncband release distribution area"
```

Export all current release-manager public keys into `KEYS`, review the file, and commit it to the release distribution directory. Append new keys instead of replacing existing valid keys.

```shell
gpg --armor --export "${ASF_GPG_FINGERPRINT}" > KEYS
svn import KEYS https://dist.apache.org/repos/dist/release/incubator/asyncband/KEYS \
  -m "Add Apache Asyncband release keys"
```

The public verification URL is <https://downloads.apache.org/incubator/asyncband/KEYS>. Update `KEYS` before staging a candidate whenever the signing keys change.

### crates.io Trusted Publishing

The `asyncband` crate already exists, so a crate owner can configure [crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing) without a bootstrap publication. In the crate's **Settings → Trusted Publishing** page, add a GitHub Actions publisher with these exact values:

| Setting           | Value         |
| ----------------- | ------------- |
| Repository owner  | `apache`      |
| Repository name   | `asyncband`   |
| Workflow filename | `release.yml` |
| Environment       | `release`     |

The workflow obtains a short-lived OIDC token and keeps no long-lived crates.io token in GitHub. The `release` GitHub environment is managed by `.asf.yaml`: it accepts version tags and requires approval from one of the configured project committers. The person who pushed the tag may approve the deployment.

After one successful Trusted Publishing release, remove any legacy `CARGO_REGISTRY_TOKEN` repository or environment secret and enable **Require trusted publishing for all new versions** in the crate settings. Keep at least two active crate owners so that the publisher configuration can be recovered without depending on one person.

## 1. Prepare the release pull request

Start from current `main` and choose `VERSION` according to the changes since the latest crates.io release.

1. Change `version` in `asyncband/Cargo.toml` and refresh `Cargo.lock` with Cargo.
2. Move the entries under `Unreleased` in `CHANGELOG.md` into a dated `VERSION` section, then restore an empty `Unreleased` section. Keep user-impacting sections ordered as breaking changes, new features, bug fixes, and improvements.
3. Confirm that `LICENSE`, `NOTICE`, and `DISCLAIMER` are correct and that all source files have the required license headers.
4. Run the normal validation and the semver check:

```shell
cargo x lint
cargo x check
cargo x test --no-capture
cargo x semver --release-version "${VERSION}"
cargo publish --package asyncband --locked --dry-run
```

For a semver-major release, including a pre-1.0 minor release such as `0.7.0`, the semver command
audits with minor compatibility rules so that breaking API changes remain visible. Review every
reported change against `CHANGELOG.md` and `MIGRATE.md`, then explicitly acknowledge the reviewed
inventory:

```shell
cargo x semver --release-version "${VERSION}" --acknowledge-breaking-changes
```

Do not use `--acknowledge-breaking-changes` for a semver-minor or semver-patch release.

Open and merge a normal pull request. Do not push the version change directly to `main`; the final release is identified by tags, so the release process does not require a direct branch push.

Record the merge commit as `RELEASE_COMMIT`. All candidate artifacts, the final tag, and the crates.io package must come from this exact commit.

## 2. Create and validate a release candidate

Fetch the merged commit, verify it, and create a signed annotated RC tag:

```shell
git fetch https://github.com/apache/asyncband.git main
git switch --detach "${RELEASE_COMMIT}"
git status --short
git tag --sign "v${VERSION}-rc.${RC}" \
  --message "Apache Asyncband ${VERSION} release candidate ${RC}" \
  "${RELEASE_COMMIT}"
git push https://github.com/apache/asyncband.git "v${VERSION}-rc.${RC}"
```

Wait for the `Release` GitHub Actions workflow to pass. An RC tag runs package validation but cannot enter the crates.io publishing job. If validation requires a code change, abandon this candidate, merge a new release PR, increment `RC`, and use a new tag; never move or reuse an existing tag.

## 3. Build the official source archive

Build the candidate from the RC tag in a clean clone. The `incubating` marker is mandatory in the filename, and `gzip -n` avoids embedding the local build time.

```shell
RC_TAG="v${VERSION}-rc.${RC}"
SOURCE_DIR="apache-asyncband-${VERSION}-incubating-src"
mkdir -p dist
git verify-tag "${RC_TAG}"
git archive --format=tar --prefix="${SOURCE_DIR}/" "${RC_TAG}" \
  | gzip -n -9 > "dist/${SOURCE_DIR}.tar.gz"
(
  cd dist
  shasum -a 512 "${SOURCE_DIR}.tar.gz" > "${SOURCE_DIR}.tar.gz.sha512"
  gpg --armor --detach-sign --local-user "${ASF_GPG_FINGERPRINT}" \
    "${SOURCE_DIR}.tar.gz"
)
```

Verify the artifacts before uploading them:

```shell
(
  cd dist
  shasum -a 512 --check "${SOURCE_DIR}.tar.gz.sha512"
  gpg --verify "${SOURCE_DIR}.tar.gz.asc" "${SOURCE_DIR}.tar.gz"
  tar --extract --gzip --file "${SOURCE_DIR}.tar.gz"
  cd "${SOURCE_DIR}"
  cargo test --workspace --all-features --locked
  cargo publish --package asyncband --locked --dry-run
)
```

Also inspect the archive for unexpected binary files, verify `LICENSE`, `NOTICE`, and `DISCLAIMER`, and check that its contents correspond to the RC tag.

## 4. Stage the candidate on ASF infrastructure

Check out an empty working copy so old candidates are not copied accidentally, add the three files, and commit them under the candidate directory:

```shell
svn checkout --depth=empty \
  https://dist.apache.org/repos/dist/dev/incubator/asyncband asyncband-dist-dev
mkdir "asyncband-dist-dev/${VERSION}-rc.${RC}"
cp "dist/${SOURCE_DIR}.tar.gz"* "asyncband-dist-dev/${VERSION}-rc.${RC}/"
svn add "asyncband-dist-dev/${VERSION}-rc.${RC}"
svn status asyncband-dist-dev
svn commit asyncband-dist-dev \
  -m "Stage Apache Asyncband ${VERSION} release candidate ${RC}"
```

Confirm that the candidate is visible at `https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}/` and that every link in the vote email works.

## 5. Hold the two-phase vote

Incubating releases require the [two-phase vote described by the Incubator](https://incubator.apache.org/cookbook/).

First, send `[VOTE] Release Apache Asyncband (Incubating) ${VERSION} RC${RC}` to `dev@asyncband.apache.org`. Include:

- the staged source URL;
- the `KEYS` URL and signing-key fingerprint;
- the signed RC tag and commit hash;
- the changelog or comparison with the previous release;
- commands or a checklist for verifying signatures, checksums, licensing, absence of unexpected binaries, and the build;
- a statement that the vote remains open for at least 72 hours.

The podling vote passes with at least three `+1` votes from PPMC members, more PPMC `+1` votes than `-1` votes, and at least 72 hours elapsed. Send a result email that identifies voters and links the archived vote thread.

Then send the same proposal to `general@incubator.apache.org`, including the podling vote result and archive link. This vote also remains open for at least 72 hours and requires at least three binding `+1` votes from Incubator PMC members with a majority in favor. Send a result email after it closes.

Do not create the final tag, publish to crates.io, or announce a release until the Incubator vote passes.

## 6. Promote and publish the approved release

Promote the exact voted artifacts from the development distribution area:

```shell
svn move \
  "https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}" \
  "https://dist.apache.org/repos/dist/release/incubator/asyncband/${VERSION}" \
  -m "Release Apache Asyncband ${VERSION}"
```

Wait for the files to appear at `https://downloads.apache.org/incubator/asyncband/${VERSION}/`. Then add a signed final tag to the exact RC commit and push only that tag:

```shell
RC_COMMIT="$(git rev-list --max-count=1 "v${VERSION}-rc.${RC}")"
git tag --sign "v${VERSION}" \
  --message "Apache Asyncband ${VERSION}" \
  "${RC_COMMIT}"
git push https://github.com/apache/asyncband.git "v${VERSION}"
```

The final tag starts the crates.io publishing job. Before approving the `release` environment deployment, a configured project committer must compare the final tag with the approved RC and confirm that the Incubator vote passed. The person who pushed the tag may perform this approval. The workflow verifies that the tag exactly matches the package version and publishes with a short-lived crates.io token.

After publication:

1. Verify the version and metadata on crates.io and docs.rs.
2. Create a GitHub release from the final tag containing release notes and links to the ASF source archive; do not attach alternate release artifacts.
3. Announce the release on `dev@asyncband.apache.org` and other appropriate channels, identifying it as Apache Asyncband (Incubating).
4. Remove superseded releases from `dist/release`; they remain available from the ASF archive.

## Failed candidates and publication failures

If either vote fails or any candidate content changes, remove the staged candidate, increment `RC`, and restart from a new signed RC tag. Never replace an artifact, checksum, signature, or tag under an existing candidate name.

```shell
svn delete \
  "https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}" \
  -m "Remove rejected Apache Asyncband ${VERSION} release candidate ${RC}"
```

If the final crates.io job fails before publication, fix only the publishing infrastructure and rerun the same workflow. If the version reached crates.io, it is immutable: do not overwrite it or move tags. Yank it only when necessary, discuss the incident on the development list, and prepare a new version through the full ASF vote when source changes are required.
