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
use std::process;

use cargo_metadata::Metadata;
use cargo_metadata::MetadataCommand;
use clap::Parser;

use crate::PACKAGE_NAME;
use crate::find_command;
use crate::run_command;

#[derive(Parser)]
pub struct CommandCheck;

impl CommandCheck {
    pub fn run(self) {
        let features = asyncband_features();

        run_command(make_check_cmd(&[]));
        for feature in features.chunks(1) {
            run_command(make_check_cmd(feature));
        }
        run_command(make_check_cmd(&features));
    }
}

fn asyncband_features() -> Vec<String> {
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

fn make_check_cmd(features: &[String]) -> process::Command {
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
