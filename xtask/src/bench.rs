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

use clap::Parser;

use crate::find_command;
use crate::run_command;

#[derive(Parser)]
pub struct CommandBench {
    #[arg(long, help = "Compile benchmarks without running them.")]
    no_run: bool,

    #[arg(
        long,
        help = "Select a target (ecosystem, primitives, or upgradable_stages; default: ecosystem and primitives)."
    )]
    bench: Option<String>,

    #[arg(
        last = true,
        conflicts_with = "no_run",
        help = "Arguments passed to the fixed-workload benchmark runner (see -- --help)."
    )]
    args: Vec<String>,
}

impl CommandBench {
    pub fn run(self) {
        run_command(make_bench_cmd(
            self.no_run,
            self.bench.as_deref(),
            &self.args,
        ));
    }
}

fn make_bench_cmd(no_run: bool, bench: Option<&str>, args: &[String]) -> StdCommand {
    let mut cmd = find_command("cargo");
    cmd.args(["bench", "--workspace", "--all-features"]);
    match bench {
        Some(bench) => cmd.args(["--bench", bench]),
        None => cmd.args(["--bench", "ecosystem", "--bench", "primitives"]),
    };
    if no_run {
        cmd.arg("--no-run");
    }
    if !args.is_empty() {
        cmd.arg("--").args(args);
    }
    cmd
}
