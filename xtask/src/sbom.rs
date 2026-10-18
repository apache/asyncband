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
use std::path::PathBuf;

use clap::Parser;
use serde_json::Value;
use serde_json::json;

use crate::find_command;
use crate::run_command;

#[derive(Parser)]
pub struct CommandSbom {
    #[arg(long, help = "Root of the extracted source release.")]
    source: PathBuf,
    #[arg(long, help = "Full Git commit of the source release.")]
    revision: String,
    #[arg(long, help = "Destination CycloneDX JSON sidecar.")]
    output: PathBuf,
}

impl CommandSbom {
    pub fn run(self) {
        assert!(
            self.revision.len() == 40 && self.revision.bytes().all(|b| b.is_ascii_hexdigit()),
            "revision must be a full Git commit"
        );
        std::env::var("SOURCE_DATE_EPOCH").expect("set SOURCE_DATE_EPOCH to the commit timestamp");
        let source = self
            .source
            .canonicalize()
            .expect("source directory missing");
        let lock = fs::read(source.join("Cargo.lock")).expect("Cargo.lock missing");
        let mut command = find_command("cargo");
        command.current_dir(&source).args([
            "cyclonedx",
            "--manifest-path",
            "asyncband/Cargo.toml",
            "--format",
            "json",
            "--spec-version",
            "1.5",
            "--all-features",
            "--target",
            "all",
        ]);
        run_command(command);
        // cargo-cyclonedx 0.5.9 does not expose Cargo's --locked flag.
        assert!(
            fs::read(source.join("Cargo.lock")).unwrap() == lock,
            "cargo-cyclonedx changed Cargo.lock"
        );

        // The tool emits a BOM for every workspace member. Publish only the library's BOM.
        let mut bom: Value = serde_json::from_slice(
            &fs::read(source.join("asyncband/asyncband.cdx.json")).expect("library BOM missing"),
        )
        .expect("invalid cargo-cyclonedx output");
        let old_ref = bom["metadata"]["component"]["bom-ref"].take();
        let reference = format!("pkg:generic/apache/asyncband@{}#asyncband", self.revision);
        let component = &mut bom["metadata"]["component"];
        component["bom-ref"] = json!(reference);
        component["purl"] = json!(reference);
        component["supplier"] =
            json!({"name": "The Apache Software Foundation", "url": ["https://www.apache.org/"]});
        // Generated target identities contain absolute paths and duplicate the library.
        component.as_object_mut().unwrap().remove("components");
        for external in component["externalReferences"].as_array_mut().unwrap() {
            if external["type"] == "vcs" {
                external["url"] = json!(format!(
                    "https://github.com/apache/asyncband/tree/{}",
                    self.revision
                ));
            }
        }
        component["externalReferences"].as_array_mut().unwrap().push(json!({
            "type": "license", "url": format!("https://github.com/apache/asyncband/blob/{}/LICENSE", self.revision),
            "comment": "Includes attribution and license terms for incorporated source portions, which are not enumerated by Cargo."
        }));
        for dependency in bom["dependencies"].as_array_mut().unwrap() {
            if dependency["ref"] == old_ref {
                dependency["ref"] = json!(reference);
            }
        }
        for component in bom["components"].as_array_mut().unwrap() {
            if component["bom-ref"]
                .as_str()
                .unwrap()
                .starts_with("registry+https://github.com/rust-lang/crates.io-index#")
            {
                // This identifies the registry distributor, not the crate's author.
                component["supplier"] = json!({"name": "crates.io", "url": ["https://crates.io/"]});
            }
        }
        bom["metadata"]["authors"] =
            json!([{"name": "Apache Asyncband", "email": "dev@asyncband.apache.org"}]);
        bom["metadata"]["lifecycles"] = json!([{"phase": "pre-build"}]);
        bom["metadata"]["properties"].as_array_mut().unwrap().push(json!({
            "name": "apache:asyncband:scope",
            "value": "Cargo normal/build dependencies of the published library, with all features and targets. Excludes dev-dependencies and other workspace members. Incorporated source portions are documented in LICENSE, not enumerated here."
        }));
        bom.sort_all_objects();
        let encoded = serde_json::to_string_pretty(&bom).unwrap() + "\n";
        assert!(
            !encoded.contains("file://"),
            "SBOM contains a machine-local path"
        );
        fs::write(&self.output, encoded).expect("failed to write SBOM");
        println!("Wrote {}", self.output.display());
    }
}
