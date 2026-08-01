// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::process::Command as StdCommand;

use clap::Parser;
use clap::Subcommand;
use semver::Version;

const CRATES_IO_API_URL: &str = "https://crates.io/api/v1/crates/mea";
const CRATES_IO_USER_AGENT: &str = "mea release tooling (https://github.com/fast/mea)";

#[derive(Parser)]
struct Command {
    #[clap(subcommand)]
    sub: SubCommand,
}

impl Command {
    fn run(self) {
        match self.sub {
            SubCommand::Build(cmd) => cmd.run(),
            SubCommand::Lint(cmd) => cmd.run(),
            SubCommand::Semver(cmd) => cmd.run(),
            SubCommand::Test(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCommand {
    #[clap(about = "Compile workspace packages.")]
    Build(CommandBuild),
    #[clap(about = "Run workspace quality checks.")]
    Lint(CommandLint),
    #[clap(about = "Verify API compatibility for a planned release.")]
    Semver(CommandSemver),
    #[clap(about = "Run unit tests.")]
    Test(CommandTest),
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
struct CommandTest {
    #[arg(long, help = "Run tests serially and do not capture output.")]
    no_capture: bool,
}

impl CommandTest {
    fn run(self) {
        run_command(make_test_cmd(self.no_capture, &[]));
    }
}

#[derive(Parser)]
struct CommandSemver {
    #[arg(long, value_name = "VERSION", help = "Version that will be released.")]
    release_version: Version,
}

impl CommandSemver {
    fn run(self) {
        let Some(baseline_version) = find_latest_release() else {
            println!("mea has not been published; skipping semver checks for the first release.");
            return;
        };

        let release_type = classify_release_type(&baseline_version, &self.release_version);
        println!(
            "Checking release {} against mea@{baseline_version} as a {} release.",
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
    let mut cmd = find_command("curl");
    cmd.args([
        "--silent",
        "--show-error",
        "--location",
        "--proto",
        "=https",
        "--connect-timeout",
        "10",
        "--max-time",
        "30",
        "--user-agent",
        CRATES_IO_USER_AGENT,
        "--write-out",
        "\n%{http_code}",
        CRATES_IO_API_URL,
    ]);
    let output = cmd.output().expect("failed to query crates.io for mea");
    assert!(
        output.status.success(),
        "failed to query crates.io for mea: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let output = String::from_utf8(output.stdout).expect("crates.io returned non-UTF-8 data");
    let (body, status) = output
        .rsplit_once('\n')
        .expect("curl did not return an HTTP status for crates.io");
    match status {
        "200" => {}
        "404" => return None,
        status => panic!("crates.io returned HTTP {status} for mea"),
    }

    let response: serde_json::Value =
        serde_json::from_str(body).expect("failed to decode crates.io response for mea");
    let crate_data = response
        .get("crate")
        .expect("crates.io response for mea did not include crate data");
    let version = crate_data
        .get("max_stable_version")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            crate_data
                .get("max_version")
                .and_then(serde_json::Value::as_str)
        })
        .expect("crates.io response for mea did not include a release version");
    Some(
        Version::parse(version)
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

fn make_test_cmd(no_capture: bool, features: &[&str]) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["test", "--workspace", "--no-default-features"]);
    if !features.is_empty() {
        cmd.args(["--features", features.join(",").as_str()]);
    }
    if no_capture {
        cmd.args(["--", "--nocapture"]);
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
        "mea",
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
        cmd.args(["format", "--fail-if-updated=false"]);
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
