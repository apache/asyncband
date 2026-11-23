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

use std::process::Command as StdCommand;
use std::time::Duration;

use clap::Parser;
use semver::Version;
use serde::Deserialize;

use crate::PACKAGE_NAME;
use crate::command_status;
use crate::ensure_installed;
use crate::find_command;

// cargo-semver-checks reserves exit code 100 for completed checks that found deny-level violations.
const SEMVER_VIOLATIONS_EXIT_CODE: i32 = 100;

#[derive(Parser)]
pub struct CommandSemver {
    #[arg(long, value_name = "VERSION", help = "Version that will be released.")]
    release_version: Version,

    #[arg(
        long,
        help = "Accept reviewed breaking API changes for a semver-major release."
    )]
    acknowledge_breaking_changes: bool,
}

impl CommandSemver {
    pub fn run(self) {
        let Some(baseline_version) = find_latest_release() else {
            println!(
                "{PACKAGE_NAME} has not been published; skipping semver checks for the first release."
            );
            return;
        };

        let release_type = classify_release_type(&baseline_version, &self.release_version);
        assert!(
            release_type == SemverReleaseType::Major || !self.acknowledge_breaking_changes,
            "--acknowledge-breaking-changes is only valid for a semver-major release"
        );

        let audit_release_type = release_type.audit_release_type();
        println!(
            "Checking release {} against {PACKAGE_NAME}@{baseline_version} as a {} release.",
            self.release_version,
            release_type.as_str()
        );
        if audit_release_type != release_type {
            println!(
                "Auditing with minor compatibility rules so breaking API changes remain visible."
            );
        }

        let status = command_status(make_semver_check_cmd(&baseline_version, audit_release_type));
        if status.success() {
            return;
        }

        if release_type == SemverReleaseType::Major
            && status.code() == Some(SEMVER_VIOLATIONS_EXIT_CODE)
        {
            assert!(
                self.acknowledge_breaking_changes,
                "breaking API changes were reported; document and review them, then rerun with \
                 --acknowledge-breaking-changes"
            );
            println!(
                "The breaking API changes reported above are acknowledged for release {}.",
                self.release_version
            );
            return;
        }

        panic!("command failed: {status}");
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

    fn audit_release_type(self) -> Self {
        match self {
            Self::Major => Self::Minor,
            release_type => release_type,
        }
    }
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

    #[test]
    fn audit_major_releases_with_minor_compatibility_rules() {
        assert_eq!(
            SemverReleaseType::Major.audit_release_type(),
            SemverReleaseType::Minor
        );
        assert_eq!(
            SemverReleaseType::Minor.audit_release_type(),
            SemverReleaseType::Minor
        );
        assert_eq!(
            SemverReleaseType::Patch.audit_release_type(),
            SemverReleaseType::Patch
        );
    }
}
