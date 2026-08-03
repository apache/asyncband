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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command as StdCommand;
use std::time::Duration;

use clap::Parser;
use clap::Subcommand;
use semver::Version;
use serde::Deserialize;

#[derive(Parser)]
struct Command {
    #[clap(subcommand)]
    sub: SubCommand,
}

impl Command {
    fn run(self) {
        match self.sub {
            SubCommand::Bench(cmd) => cmd.run(),
            SubCommand::BenchReport(cmd) => cmd.run(),
            SubCommand::Build(cmd) => cmd.run(),
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
    #[clap(about = "Compare Divan benchmark output.")]
    BenchReport(CommandBenchReport),
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
#[command(trailing_var_arg = true)]
struct CommandBench {
    #[arg(allow_hyphen_values = true)]
    args: Vec<String>,
}

impl CommandBench {
    fn run(self) {
        run_command(make_bench_cmd(&self.args));
    }
}

#[derive(Parser)]
struct CommandBenchReport {
    #[arg(long, value_name = "PATH")]
    baseline: PathBuf,
    #[arg(long, value_name = "PATH")]
    candidate: PathBuf,
    #[arg(long, value_name = "PATH")]
    output: PathBuf,
}

impl CommandBenchReport {
    fn run(self) {
        let baseline = read_divan_results(&self.baseline);
        let candidate = read_divan_results(&self.candidate);
        assert!(
            !candidate.is_empty(),
            "no Divan benchmark results found in {}",
            self.candidate.display()
        );

        let report = render_benchmark_report(&baseline, &candidate);
        fs::write(&self.output, report).unwrap_or_else(|err| {
            panic!(
                "failed to write benchmark report to {}: {err}",
                self.output.display()
            )
        });
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

fn read_divan_results(path: &Path) -> BTreeMap<String, f64> {
    let output = fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
    parse_divan_results(&output)
}

fn parse_divan_results(output: &str) -> BTreeMap<String, f64> {
    let mut groups = Vec::new();
    let mut results = BTreeMap::new();

    for line in output.lines() {
        let Some((depth, row)) = parse_divan_tree_row(line) else {
            continue;
        };

        let columns = row.split('│').collect::<Vec<_>>();
        if columns.len() < 3 {
            continue;
        }

        let Some(median_ns) = parse_duration_ns(columns[2]) else {
            let group = columns[0].trim();
            if group.is_empty() || depth > groups.len() {
                continue;
            }

            groups.truncate(depth);
            groups.push(group.to_string());
            continue;
        };

        let Some((name, _fastest_ns)) = parse_named_duration_ns(columns[0]) else {
            continue;
        };

        let mut path = groups.iter().take(depth).cloned().collect::<Vec<_>>();
        path.push(name);
        results.insert(path.join(" / "), median_ns);
    }

    results
}

fn parse_divan_tree_row(line: &str) -> Option<(usize, &str)> {
    let (branch_index, branch) = match (line.find("├─ "), line.find("╰─ ")) {
        (Some(left), Some(right)) if left < right => (left, "├─ "),
        (Some(_), Some(right)) => (right, "╰─ "),
        (Some(left), None) => (left, "├─ "),
        (None, Some(right)) => (right, "╰─ "),
        (None, None) => return None,
    };

    let prefix_width = line[..branch_index].chars().count();
    if prefix_width % 3 != 0 {
        return None;
    }

    Some((prefix_width / 3, &line[branch_index + branch.len()..]))
}

fn parse_named_duration_ns(value: &str) -> Option<(String, f64)> {
    let parts = value.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 3 {
        return None;
    }

    let duration_ns = parse_duration_parts_ns(parts[parts.len() - 2], parts[parts.len() - 1])?;
    Some((parts[..parts.len() - 2].join(" "), duration_ns))
}

fn parse_duration_ns(value: &str) -> Option<f64> {
    let mut parts = value.split_whitespace();
    let number = parts.next()?;
    let unit = parts.next()?;
    parse_duration_parts_ns(number, unit)
}

fn parse_duration_parts_ns(number: &str, unit: &str) -> Option<f64> {
    let value = number.parse::<f64>().ok()?;
    let multiplier = match unit {
        "ps" => 0.001,
        "ns" => 1.0,
        "us" | "µs" | "μs" => 1_000.0,
        "ms" => 1_000_000.0,
        "s" => 1_000_000_000.0,
        _ => return None,
    };
    Some(value * multiplier)
}

fn render_benchmark_report(
    baseline: &BTreeMap<String, f64>,
    candidate: &BTreeMap<String, f64>,
) -> String {
    let mut report = String::from("### Benchmark comparison\n\n");
    report.push_str(
        "Divan median times are compared on the same GitHub-hosted runner. Lower is better; the report is informational and does not gate CI on performance changes.\n\n",
    );

    if baseline.is_empty() {
        report.push_str(
            "> The base commit has no benchmark target yet. This run establishes the initial baseline after merge.\n\n",
        );
        report.push_str("| Benchmark | Candidate median |\n| --- | ---: |\n");
        for (name, duration_ns) in candidate {
            writeln!(
                report,
                "| {} | {} |",
                escape_markdown_table(name),
                format_duration_ns(*duration_ns)
            )
            .unwrap();
        }
        return report;
    }

    report.push_str(
        "| Benchmark | Base median | Candidate median | Change |\n| --- | ---: | ---: | ---: |\n",
    );

    let names = baseline
        .keys()
        .chain(candidate.keys())
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    for name in names {
        match (baseline.get(name), candidate.get(name)) {
            (Some(baseline_ns), Some(candidate_ns)) => {
                let change = (candidate_ns / baseline_ns - 1.0) * 100.0;
                writeln!(
                    report,
                    "| {} | {} | {} | {change:+.2}% |",
                    escape_markdown_table(name),
                    format_duration_ns(*baseline_ns),
                    format_duration_ns(*candidate_ns)
                )
                .unwrap();
            }
            (None, Some(candidate_ns)) => {
                writeln!(
                    report,
                    "| {} | — | {} | new |",
                    escape_markdown_table(name),
                    format_duration_ns(*candidate_ns)
                )
                .unwrap();
            }
            (Some(baseline_ns), None) => {
                writeln!(
                    report,
                    "| {} | {} | — | removed |",
                    escape_markdown_table(name),
                    format_duration_ns(*baseline_ns)
                )
                .unwrap();
            }
            (None, None) => unreachable!(),
        }
    }

    report
}

fn format_duration_ns(duration_ns: f64) -> String {
    if duration_ns < 1.0 {
        format!("{:.3} ps", duration_ns * 1_000.0)
    } else if duration_ns < 1_000.0 {
        format!("{duration_ns:.3} ns")
    } else if duration_ns < 1_000_000.0 {
        format!("{:.3} µs", duration_ns / 1_000.0)
    } else if duration_ns < 1_000_000_000.0 {
        format!("{:.3} ms", duration_ns / 1_000_000.0)
    } else {
        format!("{:.3} s", duration_ns / 1_000_000_000.0)
    }
}

fn escape_markdown_table(value: &str) -> String {
    value.replace('|', "\\|")
}

fn find_latest_release() -> Option<Version> {
    let agent = ureq::Agent::from(
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .build(),
    );

    let mut response = match agent.get("https://crates.io/api/v1/crates/mea").call() {
        Ok(response) => response,
        Err(ureq::Error::StatusCode(404)) => return None,
        Err(err) => panic!("failed to query crates.io for mea: {err}"),
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

    let response: CratesIoResponse = response
        .body_mut()
        .read_json()
        .expect("failed to decode crates.io response for mea");
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

fn make_bench_cmd(args: &[String]) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["bench", "--workspace", "--bench", "*"]);
    if !args.is_empty() {
        cmd.arg("--").args(args);
    }
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
    fn parses_nested_divan_medians_in_nanoseconds() {
        let output = r#"
Timer precision: 41 ns
primitives                 fastest       │ slowest       │ median        │ mean
╰─ oneshot                               │               │               │
   ├─ poll_before_send     21.87 ns      │ 382.5 ns      │ 23.18 ns      │ 28.27 ns
   ╰─ send_before_poll     900 ns        │ 2.1 µs        │ 1.5 µs        │ 1.6 µs
"#;

        let results = parse_divan_results(output);
        assert_eq!(results.len(), 2);
        assert_eq!(results["oneshot / poll_before_send"], 23.18);
        assert_eq!(results["oneshot / send_before_poll"], 1_500.0);
    }

    #[test]
    fn benchmark_report_compares_shared_and_changed_cases() {
        let baseline = BTreeMap::from([
            ("oneshot / removed".to_string(), 10.0),
            ("oneshot / shared".to_string(), 20.0),
        ]);
        let candidate = BTreeMap::from([
            ("oneshot / new".to_string(), 30.0),
            ("oneshot / shared".to_string(), 25.0),
        ]);

        let report = render_benchmark_report(&baseline, &candidate);
        assert_eq!(
            report,
            concat!(
                "### Benchmark comparison\n\n",
                "Divan median times are compared on the same GitHub-hosted runner. Lower is better; the report is informational and does not gate CI on performance changes.\n\n",
                "| Benchmark | Base median | Candidate median | Change |\n",
                "| --- | ---: | ---: | ---: |\n",
                "| oneshot / new | — | 30.000 ns | new |\n",
                "| oneshot / removed | 10.000 ns | — | removed |\n",
                "| oneshot / shared | 20.000 ns | 25.000 ns | +25.00% |\n",
            )
        );
    }

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
