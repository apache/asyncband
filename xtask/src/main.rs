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

use std::process;

use clap::Parser;
use clap::Subcommand;

use crate::bench::CommandBench;
use crate::build::CommandBuild;
use crate::check::CommandCheck;
use crate::lint::CommandLint;
use crate::miri::CommandMiri;
use crate::semver::CommandSemver;
use crate::test::CommandTest;

mod bench;
mod build;
mod check;
mod lint;
mod miri;
mod semver;
mod test;

const PACKAGE_NAME: &str = "asyncband";

fn main() {
    let cmd = Command::parse();
    cmd.run();
}

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
            SubCommand::Miri(cmd) => cmd.run(),
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
    #[clap(about = "Check memory safety with Miri.")]
    Miri(CommandMiri),
    #[clap(about = "Verify API compatibility for a planned release.")]
    Semver(CommandSemver),
    #[clap(about = "Run workspace tests.")]
    Test(CommandTest),
}

fn find_command(cmd: &str) -> process::Command {
    match which::which(cmd) {
        Ok(exe) => {
            let mut cmd = process::Command::new(exe);
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

fn run_command(cmd: process::Command) {
    let status = command_status(cmd);
    assert!(status.success(), "command failed: {status}");
}

fn command_status(mut cmd: process::Command) -> process::ExitStatus {
    println!("{cmd:?}");
    cmd.status().expect("failed to execute process")
}
