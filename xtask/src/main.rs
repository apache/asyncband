// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::Path;
use std::process::Command as StdCommand;
use std::time::Duration;

use clap::Parser;
use clap::Subcommand;
use semver::Version;
use serde::Deserialize;

const PACKAGE_NAME: &str = "asyncband";

#[derive(Parser)]
struct Command {
    #[clap(subcommand)]
    sub: SubCommand,
}

impl Command {
    fn run(self) {
        match self.sub {
            SubCommand::Bench(cmd) => cmd.run(),
            SubCommand::Build(cmd) => cmd.run(),
            SubCommand::Check(cmd) => cmd.run(),
            SubCommand::Lint(cmd) => cmd.run(),
            SubCommand::Semver(cmd) => cmd.run(),
            SubCommand::Test(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCommand {
    #[clap(about = "Run workspace benchmarks.")]
    Bench(CommandBench),
    #[clap(about = "Compile workspace packages.")]
    Build(CommandBuild),
    #[clap(about = "Check asyncband under the feature matrix.")]
    Check(CommandCheck),
    #[clap(about = "Run workspace quality checks.")]
    Lint(CommandLint),
    #[clap(about = "Verify API compatibility for a planned release.")]
    Semver(CommandSemver),
    #[clap(about = "Run unit tests.")]
    Test(CommandTest),
}

#[derive(Parser)]
struct CommandBench;

impl CommandBench {
    fn run(self) {
        run_command(make_bench_cmd());
    }
}

#[derive(Parser)]
struct CommandBuild {
    #[arg(long, help = "Assert that `Cargo.lock` will remain unchanged.")]
    locked: bool,
}

impl CommandBuild {
    fn run(self) {
        run_command(make_build_cmd(self.locked));
    }
}

#[derive(Parser)]
struct CommandCheck;

impl CommandCheck {
    fn run(self) {
        let features = asyncband_features();

        run_command(make_check_cmd(&[]));
        for feature in features.chunks(1) {
            run_command(make_check_cmd(feature));
        }
        run_command(make_check_cmd(&features));
    }
}

#[derive(Parser)]
struct CommandTest {
    #[arg(long, help = "Run tests serially and do not capture output.")]
    no_capture: bool,
}

impl CommandTest {
    fn run(self) {
        run_command(make_test_cmd(self.no_capture, &asyncband_features()));
    }
}

fn asyncband_features() -> Vec<String> {
    use cargo_metadata::Metadata;
    use cargo_metadata::MetadataCommand;

    let manifest = Path::new(env!("CARGO_WORKSPACE_DIR")).join("Cargo.toml");
    let Metadata { packages, .. } = MetadataCommand::new()
        .manifest_path(manifest)
        .exec()
        .expect("failed to get cargo metadata");
    let package = packages
        .into_iter()
        .find(|package| package.name == PACKAGE_NAME)
        .expect("failed to find asyncband package");

    let mut features = package
        .features
        .into_keys()
        .filter(|feature| feature != "default")
        .collect::<Vec<_>>();
    features.sort();
    features
}

#[derive(Parser)]
struct CommandSemver {
    #[arg(long, value_name = "VERSION", help = "Version that will be released.")]
    release_version: Version,
}

impl CommandSemver {
    fn run(self) {
        let Some(baseline_version) = find_latest_release() else {
            println!(
                "{PACKAGE_NAME} has not been published; skipping semver checks for the first release."
            );
            return;
        };

        let release_type = classify_release_type(&baseline_version, &self.release_version);
        println!(
            "Checking release {} against {PACKAGE_NAME}@{baseline_version} as a {} release.",
            self.release_version,
            release_type.as_str()
        );
        run_command(make_semver_check_cmd(&baseline_version, release_type));
    }
}

#[derive(Parser)]
#[clap(name = "lint")]
struct CommandLint {
    #[arg(long, help = "Automatically apply lint suggestions.")]
    fix: bool,
}

impl CommandLint {
    fn run(self) {
        run_command(make_clippy_cmd(self.fix));
        run_command(make_format_cmd(self.fix));
        run_command(make_taplo_cmd(self.fix));
        run_command(make_typos_cmd());
        run_command(make_hawkeye_cmd(self.fix));
        run_command(make_doc_cmd());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SemverReleaseType {
    Major,
    Minor,
    Patch,
}

impl SemverReleaseType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Major => "major",
            Self::Minor => "minor",
            Self::Patch => "patch",
        }
    }
}

fn find_command(cmd: &str) -> StdCommand {
    match which::which(cmd) {
        Ok(exe) => {
            let mut cmd = StdCommand::new(exe);
            cmd.current_dir(env!("CARGO_WORKSPACE_DIR"));
            cmd
        }
        Err(err) => {
            panic!("{cmd} not found: {err}");
        }
    }
}

fn ensure_installed(bin: &str, crate_name: &str) {
    if which::which(bin).is_err() {
        let mut cmd = find_command("cargo");
        cmd.args(["install", crate_name]);
        run_command(cmd);
    }
}

fn run_command(mut cmd: StdCommand) {
    println!("{cmd:?}");
    let status = cmd.status().expect("failed to execute process");
    assert!(status.success(), "command failed: {status}");
}

fn find_latest_release() -> Option<Version> {
    let agent = ureq::Agent::from(
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .build(),
    );

    let url = format!("https://crates.io/api/v1/crates/{PACKAGE_NAME}");
    let mut response = match agent.get(&url).call() {
        Ok(response) => response,
        Err(ureq::Error::StatusCode(404)) => return None,
        Err(err) => panic!("failed to query crates.io for {PACKAGE_NAME}: {err}"),
    };

    #[derive(Deserialize)]
    struct CratesIoResponse {
        #[serde(rename = "crate")]
        crate_data: CratesIoCrate,
    }

    #[derive(Deserialize)]
    struct CratesIoCrate {
        max_version: String,
        max_stable_version: Option<String>,
    }

    let response: CratesIoResponse = response.body_mut().read_json().unwrap_or_else(|err| {
        panic!("failed to decode crates.io response for {PACKAGE_NAME}: {err}")
    });
    let version = response
        .crate_data
        .max_stable_version
        .unwrap_or(response.crate_data.max_version);
    Some(
        Version::parse(&version)
            .unwrap_or_else(|err| panic!("crates.io returned invalid version {version:?}: {err}")),
    )
}

fn classify_release_type(baseline: &Version, release: &Version) -> SemverReleaseType {
    assert!(
        baseline.cmp_precedence(release).is_lt(),
        "release version {release} must be greater than baseline {baseline}"
    );

    if baseline.major != release.major {
        SemverReleaseType::Major
    } else if baseline.minor != release.minor {
        if release.major == 0 {
            SemverReleaseType::Major
        } else {
            SemverReleaseType::Minor
        }
    } else if baseline.patch != release.patch {
        match (release.major, release.minor) {
            (0, 0) => SemverReleaseType::Major,
            (0, _) => SemverReleaseType::Minor,
            _ => SemverReleaseType::Patch,
        }
    } else {
        SemverReleaseType::Major
    }
}

fn make_bench_cmd() -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["bench", "--workspace", "--all-features", "--bench", "*"]);
    cmd
}

fn make_build_cmd(locked: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args([
        "build",
        "--workspace",
        "--all-features",
        "--tests",
        "--examples",
        "--benches",
        "--bins",
    ]);
    if locked {
        cmd.arg("--locked");
    }
    cmd
}

fn make_test_cmd(no_capture: bool, features: &[String]) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["test", "--workspace", "--no-default-features"]);
    for feature in features {
        cmd.args(["--features", feature]);
    }
    if no_capture {
        cmd.args(["--", "--nocapture"]);
    }
    cmd
}

fn make_check_cmd(features: &[String]) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.env("RUSTFLAGS", "-Dwarnings");
    cmd.args([
        "+nightly",
        "check",
        "--package",
        PACKAGE_NAME,
        "--all-targets",
        "--no-default-features",
    ]);
    for feature in features {
        cmd.args(["--features", feature]);
    }
    cmd
}

fn make_format_cmd(fix: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["+nightly", "fmt", "--all"]);
    if !fix {
        cmd.arg("--check");
    }
    cmd
}

fn make_clippy_cmd(fix: bool) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args([
        "+nightly",
        "clippy",
        "--tests",
        "--all-features",
        "--all-targets",
        "--workspace",
    ]);
    if fix {
        cmd.args(["--allow-staged", "--allow-dirty", "--fix"]);
    } else {
        cmd.args(["--", "-D", "warnings"]);
    }
    cmd
}

fn make_doc_cmd() -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.env("RUSTDOCFLAGS", "-D warnings --cfg docsrs");
    cmd.args([
        "+nightly",
        "doc",
        "--workspace",
        "--all-features",
        "--no-deps",
    ]);
    cmd
}

fn make_semver_check_cmd(
    baseline_version: &Version,
    release_type: SemverReleaseType,
) -> StdCommand {
    ensure_installed("cargo-semver-checks", "cargo-semver-checks");
    let mut cmd = find_command("cargo");
    cmd.args([
        "+stable",
        "semver-checks",
        "check-release",
        "--package",
        PACKAGE_NAME,
        "--all-features",
        "--baseline-version",
    ])
    .arg(baseline_version.to_string())
    .args(["--release-type", release_type.as_str()]);
    cmd
}

fn make_hawkeye_cmd(fix: bool) -> StdCommand {
    ensure_installed("hawkeye", "hawkeye");
    let mut cmd = find_command("hawkeye");
    if fix {
        cmd.args(["format"]);
    } else {
        cmd.args(["check"]);
    }
    cmd
}

fn make_typos_cmd() -> StdCommand {
    ensure_installed("typos", "typos-cli");
    find_command("typos")
}

fn make_taplo_cmd(fix: bool) -> StdCommand {
    ensure_installed("taplo", "taplo-cli");
    let mut cmd = find_command("taplo");
    if fix {
        cmd.args(["format"]);
    } else {
        cmd.args(["format", "--check"]);
    }
    cmd
}

fn main() {
    let cmd = Command::parse();
    cmd.run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_release_types_with_cargo_pre_one_semantics() {
        let cases = [
            ("0.0.1", "0.0.2", SemverReleaseType::Major),
            ("0.6.5", "0.6.6", SemverReleaseType::Minor),
            ("0.6.5", "0.7.0", SemverReleaseType::Major),
            ("1.2.3", "1.2.4", SemverReleaseType::Patch),
            ("1.2.3", "1.3.0", SemverReleaseType::Minor),
            ("1.2.3", "2.0.0", SemverReleaseType::Major),
        ];

        for (baseline, release, expected) in cases {
            assert_eq!(
                classify_release_type(
                    &Version::parse(baseline).unwrap(),
                    &Version::parse(release).unwrap()
                ),
                expected
            );
        }
    }
}
