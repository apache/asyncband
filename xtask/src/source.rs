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

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;

use cargo_metadata::MetadataCommand;
use clap::Parser;

use crate::PACKAGE_NAME;
use crate::find_command;
use crate::run_command;

#[derive(Parser)]
pub struct CommandSource {
    #[arg(
        long,
        help = "New directory for the source archive and SHA-512 checksum."
    )]
    output: PathBuf,

    #[arg(
        long,
        help = "Compare the generated archive with a downloaded candidate."
    )]
    verify: Option<PathBuf>,
}

impl CommandSource {
    pub fn run(self) {
        // The manifest read by Cargo must describe the same tree that Git archives.
        let mut clean = find_command("git");
        clean.args(["diff", "--quiet", "HEAD", "--"]);
        run_command(clean);

        let metadata = MetadataCommand::new()
            .manifest_path(Path::new(env!("CARGO_WORKSPACE_DIR")).join("Cargo.toml"))
            .no_deps()
            .other_options(vec!["--locked".to_owned()])
            .exec()
            .expect("failed to read workspace metadata");
        let version = &metadata
            .packages
            .iter()
            .find(|package| package.name == PACKAGE_NAME)
            .expect("asyncband package missing")
            .version;
        assert!(
            version.pre.is_empty() && version.build.is_empty(),
            "expected a stable X.Y.Z package version"
        );

        let mut revision = find_command("git");
        revision.args(["rev-parse", "--verify", "HEAD^{commit}"]);
        let revision = output(revision);
        let revision = revision.trim();
        let mut git_version = find_command("git");
        git_version.arg("--version");
        let mut gzip_version = find_command("gzip");
        gzip_version.arg("--version");
        let gzip_version = output(gzip_version);
        assert!(
            gzip_version.contains("Free Software Foundation"),
            "GNU gzip is required; on macOS install it with `brew install gzip` and put it on PATH"
        );
        println!(
            "Commit: {revision}\nVersion: {version}\n{}{}",
            output(git_version),
            gzip_version
        );

        fs::create_dir(&self.output)
            .expect("output must be a new directory with an existing parent");
        let directory = fs::canonicalize(&self.output).expect("failed to resolve output directory");
        let name = format!("apache-asyncband-{version}-incubating-src.tar.gz");
        let archive_path = directory.join(&name);
        let archive_file = fs::File::create_new(&archive_path).expect("failed to create archive");

        // Archive the commit, not the working tree. Git supplies ordering and commit timestamps.
        let mut archive = find_command("git");
        archive
            .args(["-c", "tar.umask=0022", "archive", "--format=tar"])
            .arg(format!(
                "--prefix=apache-asyncband-{version}-incubating-src/"
            ))
            .arg(revision)
            .stdout(Stdio::piped());
        let mut archive = archive.spawn().expect("failed to start git archive");
        let mut gzip = find_command("gzip");
        gzip.env_remove("GZIP")
            .args(["-n", "-9"])
            .stdin(archive.stdout.take().expect("archive pipe missing"))
            .stdout(archive_file);
        let compressed = gzip.status().expect("failed to run gzip");
        let archived = archive.wait().expect("failed to wait for git archive");
        assert!(
            archived.success() && compressed.success(),
            "source archive generation failed"
        );

        let mut checksum = find_command("shasum");
        checksum.current_dir(&directory).args(["-a", "512", &name]);
        let checksum = output(checksum);
        fs::write(directory.join(format!("{name}.sha512")), &checksum)
            .expect("failed to write checksum");
        print!("{checksum}");

        if let Some(candidate) = self.verify {
            let mut compare = find_command("cmp");
            compare
                .arg(&archive_path)
                .arg(fs::canonicalize(candidate).expect("failed to resolve downloaded candidate"));
            run_command(compare);
            println!(
                "The downloaded candidate is byte-for-byte identical to the reproduced source archive."
            );
        }
    }
}

fn output(mut command: Command) -> String {
    let result = command.output().expect("failed to execute command");
    assert!(
        result.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).expect("command output is not UTF-8")
}
