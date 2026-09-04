# Releasing Apache Asyncband (Incubating)

This runbook is for release managers. Follow the current [ASF Release Policy](https://www.apache.org/legal/release-policy.html), [ASF Release Distribution Policy](https://infra.apache.org/release-distribution), [ASF Release Creation Process](https://infra.apache.org/release-publishing.html), and [Incubator release guidance](https://incubator.apache.org/guides/releasemanagement.html); the current ASF policies are authoritative.

The signed source archive approved by the Apache Incubator PMC and published through ASF distribution is the official Apache release. The crates.io package is a convenience distribution from the same approved commit. A signed `vX.Y.Z-rc.N` tag identifies the candidate; after the PPMC and IPMC votes pass, the voted artifacts move to the release distribution area and a signed `vX.Y.Z` tag on the same commit triggers crates.io publication.

## Interim non-ASF 0.7.1 release

Version 0.7.1 is a one-time interim non-ASF release. It is not approved by the Apache Incubator PMC, is not an act of the ASF, and must not be staged or published through ASF release infrastructure.

Prepare 0.7.1 on `main`, retain `DISCLAIMER-WIP` in the Cargo package, and run the normal release checks against the exact release commit. Create and push a signed `v0.7.1` tag whose message identifies it as a non-ASF release; the release workflow validates the package but intentionally skips its crates.io publishing job for this tag. After that validation passes, publish the crate manually from a maintainer-controlled environment and verify crates.io and docs.rs. Do not upload 0.7.1 artifacts to ASF distribution or announce it as an Apache release.

The next ASF release starts from a later version and follows the remainder of this runbook, including a fresh candidate and both release votes.

## Conventions

- `VERSION` is the proposed final version, for example `0.7.0`.
- `RC` is the positive candidate number, for example `1`.
- `RELEASE_COMMIT` is the merged release pull request commit used for every candidate artifact and release tag.
- The source archive is `apache-asyncband-${VERSION}-incubating-src.tar.gz`; the RC number appears in the staging directory and tag.

## One-time setup

### Signing and ASF distribution

The release manager needs an ASF-associated GPG key in the project `KEYS` file. Follow the [ASF release signing guide](https://infra.apache.org/release-signing.html), publish the public key, and verify its fingerprint through an independent channel.

Create the project distribution directories before the first release:

```shell
svn mkdir --parents https://dist.apache.org/repos/dist/dev/incubator/asyncband \
  -m "Initialize Apache Asyncband development distribution area"
svn mkdir --parents https://dist.apache.org/repos/dist/release/incubator/asyncband \
  -m "Initialize Apache Asyncband release distribution area"
```

Initialize `KEYS` with the first release manager's public key:

```shell
KEYS_FILE="$(mktemp)"
gpg --armor --export "${ASF_GPG_FINGERPRINT}" > "${KEYS_FILE}"
svn import "${KEYS_FILE}" https://dist.apache.org/repos/dist/release/incubator/asyncband/KEYS \
  -m "Add Apache Asyncband release keys"
rm "${KEYS_FILE}"
```

Add each later release manager's public key to the existing `KEYS` file through the release distribution repository before staging their first candidate. The public verification URL is <https://downloads.apache.org/incubator/asyncband/KEYS>.

### crates.io Trusted Publishing

A crate owner configures [crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing) for `asyncband` with these values:

| Setting           | Value         |
| ----------------- | ------------- |
| Repository owner  | `apache`      |
| Repository name   | `asyncband`   |
| Workflow filename | `release.yml` |
| Environment       | `release`     |

The `release` environment in `.asf.yaml` limits deployments to version tags and requires a configured reviewer. After validating the first OIDC publication, enable **Require trusted publishing for all new versions** in the crate settings.

## 1. Prepare the release pull request

Start from current `main` and choose `VERSION` from the changes since the latest crates.io release.

1. Change `version` in `asyncband/Cargo.toml` and refresh `Cargo.lock` with Cargo.
2. Move the entries under `Unreleased` in `CHANGELOG.md` into an undated `v${VERSION}` section immediately below it, then restore an empty `Unreleased` section. Keep user-impacting sections ordered as breaking changes, new features, bug fixes, and improvements; add the actual release date only after publication.
3. Verify `LICENSE`, `NOTICE`, the applicable `DISCLAIMER` or `DISCLAIMER-WIP`, source headers, and bundled dependencies.
4. Run the release checks:

```shell
cargo x lint
cargo x check
cargo x test --no-capture
RUSTUP_TOOLCHAIN=1.86.0 cargo x test --no-capture
cargo x semver --release-version "${VERSION}"
cargo publish --package asyncband --locked --dry-run
```

For a semver-major release, including a pre-1.0 minor release such as `0.7.0`, the semver command uses minor compatibility rules to report breaking API changes. When it reports expected changes, record and review them in `CHANGELOG.md`, then rerun:

```shell
cargo x semver --release-version "${VERSION}" --acknowledge-breaking-changes
```

Merge the release pull request and record its merge commit as `RELEASE_COMMIT`. Every candidate artifact, the final tag, and the crates.io package use this exact commit.

## 2. Create and validate a release candidate

Fetch `RELEASE_COMMIT`, inspect the detached worktree, and create a signed annotated RC tag:

```shell
RC_TAG="v${VERSION}-rc.${RC}"
git fetch https://github.com/apache/asyncband.git main
git switch --detach "${RELEASE_COMMIT}"
git grep -F "version = \"${VERSION}\"" -- asyncband/Cargo.toml Cargo.lock
git grep -Fx "## v${VERSION}" -- CHANGELOG.md
test -z "$(git status --porcelain)"
git tag --sign "${RC_TAG}" \
  --message "Apache Asyncband ${VERSION} release candidate ${RC}" \
  "${RELEASE_COMMIT}"
git push https://github.com/apache/asyncband.git "${RC_TAG}"
```

Wait for the `Release` GitHub Actions workflow to pass. The workflow publishes a convenience prerelease named `${VERSION}-rc.${RC}` to crates.io while the source tag and release candidate retain the stable `${VERSION}` package version. A candidate that needs a code change gets a new release pull request, merge commit, RC number, and signed tag.

## 3. Build and verify the source archive

Build the source archive from the verified RC tag. The `incubating` marker is required in the filename, and `gzip -n` keeps the gzip header independent of the local build time.

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

Verify the artifacts in a fresh temporary directory:

```shell
VERIFY_DIR="$(mktemp -d)"
(
  cd dist
  shasum -a 512 --check "${SOURCE_DIR}.tar.gz.sha512"
  gpg --verify "${SOURCE_DIR}.tar.gz.asc" "${SOURCE_DIR}.tar.gz"
  tar --extract --gzip --file "${SOURCE_DIR}.tar.gz" --directory "${VERIFY_DIR}"
)
(
  cd "${VERIFY_DIR}/${SOURCE_DIR}"
  cargo test --workspace --all-features --locked
  cargo publish --package asyncband --locked --dry-run
)
rm -rf "${VERIFY_DIR}"
```

Inspect the archive for unexpected binary files and compare its contents with the RC tag. Read `LICENSE` and `NOTICE` against the bundled and derived third-party works and their source-file notices; the presence of those files and a successful automated header scan are not sufficient verification.

## 4. Stage the candidate on ASF infrastructure

Check out an empty working copy, add the three candidate files under the RC directory, and commit them:

```shell
svn checkout --depth=empty \
  https://dist.apache.org/repos/dist/dev/incubator/asyncband asyncband-dist-dev
mkdir "asyncband-dist-dev/${VERSION}-rc.${RC}"
cp \
  "dist/${SOURCE_DIR}.tar.gz" \
  "dist/${SOURCE_DIR}.tar.gz.asc" \
  "dist/${SOURCE_DIR}.tar.gz.sha512" \
  "asyncband-dist-dev/${VERSION}-rc.${RC}/"
svn add "asyncband-dist-dev/${VERSION}-rc.${RC}"
svn status asyncband-dist-dev
svn commit asyncband-dist-dev \
  -m "Stage Apache Asyncband ${VERSION} release candidate ${RC}"
```

Confirm the candidate at `https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}/` and verify every link prepared for the vote email.

## 5. Hold the two-phase vote

Incubating releases use the [Incubator two-phase vote](https://incubator.apache.org/cookbook/#two-phase-vote-on-podling-releases). Each vote remains open for at least 72 hours.

First, send `[VOTE] Release Apache Asyncband (Incubating) ${VERSION} RC${RC}` to `dev@asyncband.apache.org`. Include:

- the staged source URL;
- the `KEYS` URL and signing-key fingerprint;
- the signed RC tag and commit hash;
- the changelog or comparison with the previous release;
- verification commands or a checklist for signatures, checksums, licensing, unexpected binaries, and the build;
- a closing time at least 72 hours after the vote starts.

The PPMC vote passes with at least three PPMC `+1` votes and more PPMC `+1` votes than `-1` votes. Publish a result email that identifies the voters and links the archived vote thread.

Then send the proposal to `general@incubator.apache.org` with the PPMC result and archive link. The IPMC vote passes with at least three binding IPMC `+1` votes and more binding `+1` votes than `-1` votes. Publish its result email and record the archive link.

Begin publication after the IPMC result records a passing vote.

## 6. Promote and publish the approved release

Promote the exact voted artifacts from the development distribution area:

```shell
svn move \
  "https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}" \
  "https://dist.apache.org/repos/dist/release/incubator/asyncband/${VERSION}" \
  -m "Release Apache Asyncband ${VERSION}"
```

Create the signed final tag from the verified RC tag and push it:

```shell
RC_TAG="v${VERSION}-rc.${RC}"
git verify-tag "${RC_TAG}"
git tag --sign "v${VERSION}" \
  --message "Apache Asyncband ${VERSION}" \
  "${RC_TAG}^{commit}"
git push https://github.com/apache/asyncband.git "v${VERSION}"
```

The final tag starts the crates.io publishing job. A configured reviewer compares the final tag with the approved RC, confirms the IPMC vote result, and approves the `release` environment deployment. The workflow verifies the package version and publishes with a short-lived crates.io token.

After publication:

1. Verify the version and metadata on crates.io and docs.rs.
2. After ASF distribution syncs, verify the source archive, checksum, and signature under `https://downloads.apache.org/incubator/asyncband/${VERSION}/` and the project `KEYS` file at `https://downloads.apache.org/incubator/asyncband/KEYS`.
3. Submit a post-release pull request that adds the actual publication date to the `v${VERSION}` changelog heading.
4. Announce the release on `dev@asyncband.apache.org` and other appropriate channels as Apache Asyncband (Incubating).
5. Remove superseded releases from `dist/release`; ASF retains them in the archive.

## Recover from failures

A failed vote or changed candidate content starts a new candidate with an incremented `RC`. Remove the rejected candidate from the development distribution area:

```shell
svn delete \
  "https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}" \
  -m "Remove rejected Apache Asyncband ${VERSION} release candidate ${RC}"
```

For a failed final crates.io job, first check whether the version exists on crates.io. Rerun the same workflow for a transient or publishing-infrastructure failure when the version is absent. A source or package change uses a new version and the full ASF vote process. A version present on crates.io is immutable; discuss any yank and follow-up release on the development list.
