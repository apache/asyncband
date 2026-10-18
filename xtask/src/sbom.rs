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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use cargo_metadata::CargoOpt;
use cargo_metadata::DependencyKind;
use cargo_metadata::Metadata;
use cargo_metadata::MetadataCommand;
use clap::Parser;
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::Value;
use serde_json::json;
use sha2::Digest;
use sha2::Sha512;

use crate::PACKAGE_NAME;
use crate::find_command;
use crate::run_command;

const GENERATOR_VERSION: &str = "0.5.9";
const CRATES_IO: &str = "registry+https://github.com/rust-lang/crates.io-index#";

#[derive(Parser)]
pub struct CommandSbom {
    #[arg(long, help = "Source release produced by git archive and gzip.")]
    archive: PathBuf,
    #[arg(long, help = "Destination CycloneDX JSON sidecar.")]
    output: PathBuf,
}

impl CommandSbom {
    pub fn run(self) {
        self.generate().expect("failed to compose the source SBOM");
    }

    fn generate(self) -> Result<()> {
        let tool = find_command("cargo")
            .args(["cyclonedx", "--version"])
            .output()?;
        ensure!(
            tool.status.success()
                && String::from_utf8(tool.stdout)?.split_whitespace().last()
                    == Some(GENERATOR_VERSION),
            "install cargo-cyclonedx {GENERATOR_VERSION} with cargo install --locked"
        );
        let filename = self
            .archive
            .file_name()
            .context("missing archive filename")?
            .to_str()
            .context("non-UTF-8 archive filename")?;
        let prefix = filename
            .strip_suffix(".tar.gz")
            .context("expected a .tar.gz archive")?;
        let bytes = fs::read(&self.archive)?;
        let checksum = format!("{:x}", Sha512::digest(&bytes));
        let temporary = tempfile::tempdir()?;
        let (commit, epoch) = extract(&bytes, prefix, temporary.path())?;
        let root = temporary.path().join(prefix).canonicalize()?;
        let lock = fs::read(root.join("Cargo.lock"))?;
        let metadata = MetadataCommand::new()
            .manifest_path(root.join("asyncband/Cargo.toml"))
            .features(CargoOpt::AllFeatures)
            .other_options(vec!["--locked".into()])
            .exec()?;
        let package = metadata
            .packages
            .iter()
            .find(|p| p.name == PACKAGE_NAME && p.source.is_none())
            .context("asyncband package missing")?;
        ensure!(
            filename == format!("apache-asyncband-{}-incubating-src.tar.gz", package.version),
            "archive name does not match the package version"
        );
        let sources = read_sources(&fs::read_to_string(root.join("LICENSE"))?)?;
        for source in &sources {
            for file in source.files.keys() {
                let path = root
                    .join(file)
                    .canonicalize()
                    .with_context(|| format!("LICENSE names a missing file: {file}"))?;
                ensure!(
                    path.starts_with(&root) && path.is_file(),
                    "LICENSE source is outside the archive: {file}"
                );
            }
        }
        let mut command = find_command("cargo");
        command
            .current_dir(&root)
            .env("SOURCE_DATE_EPOCH", epoch.to_string())
            .env("CARGO_NET_OFFLINE", "true")
            .args([
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
        ensure!(
            fs::read(root.join("Cargo.lock"))? == lock,
            "cargo-cyclonedx changed Cargo.lock"
        );
        // The generator emits one BOM per member. Only the published library's
        // normal/build dependency graph belongs in this inventory.
        let mut bom: Value =
            serde_json::from_slice(&fs::read(root.join("asyncband/asyncband.cdx.json"))?)?;
        compose(&mut bom, &metadata, &sources, &commit, filename, &checksum)?;
        bom.sort_all_objects();
        let encoded = serde_json::to_string_pretty(&bom)? + "\n";
        ensure!(
            !encoded.contains("file://")
                && !encoded.contains(root.to_str().context("non-UTF-8 extraction path")?),
            "SBOM contains a machine-local path"
        );
        fs::write(&self.output, encoded)?;
        println!("Wrote {}", self.output.display());
        Ok(())
    }
}

fn extract(bytes: &[u8], prefix: &str, destination: &Path) -> Result<(String, u64)> {
    let mut archive = tar::Archive::new(GzDecoder::new(bytes));
    let mut commit = String::new();
    let mut epoch = None;
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type().is_pax_global_extensions() {
            for extension in entry
                .pax_extensions()?
                .context("missing git archive header")?
            {
                let extension = extension?;
                if extension.key()? == "comment" {
                    commit = extension.value()?.to_owned();
                }
            }
            continue;
        }
        ensure!(
            entry.path()?.starts_with(prefix),
            "archive must have one root matching its filename"
        );
        epoch.get_or_insert(entry.header().mtime()?);
        ensure!(
            entry.unpack_in(destination)?,
            "archive path escapes the extraction directory"
        );
    }
    ensure!(
        commit.len() == 40 && commit.bytes().all(|b| b.is_ascii_hexdigit()),
        "expected the commit recorded by git archive"
    );
    Ok((commit, epoch.context("empty source archive")?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    name: String,
    version: String,
    license: String,
    upstream: Vec<String>,
    files: BTreeMap<String, Vec<String>>,
}

fn read_sources(license: &str) -> Result<Vec<Source>> {
    let mut sources = Vec::new();
    let mut record = None;
    for line in license.lines() {
        match line {
            "BEGIN SOURCE" => {
                ensure!(record.is_none(), "nested LICENSE source record");
                record = Some(String::new());
            }
            "END SOURCE" => {
                let source: Source =
                    toml::from_str(&record.take().context("unmatched END SOURCE")?)?;
                ensure!(
                    !source.name.is_empty()
                        && !source.version.is_empty()
                        && !source.license.is_empty()
                        && !source.upstream.is_empty()
                        && !source.files.is_empty()
                        && source.files.values().all(|paths| !paths.is_empty()),
                    "incomplete LICENSE source record"
                );
                sources.push(source);
            }
            _ => {
                if let Some(record) = &mut record {
                    record.push_str(line);
                    record.push('\n');
                }
            }
        }
    }
    ensure!(record.is_none(), "unterminated LICENSE source record");
    ensure!(!sources.is_empty(), "LICENSE has no source records");
    Ok(sources)
}

fn asf() -> Value {
    json!({"name": "The Apache Software Foundation", "url": ["https://www.apache.org/"]})
}

fn property(name: &str, value: impl ToString) -> Value {
    json!({"name": format!("apache:asyncband:{name}"), "value": value.to_string()})
}

fn compose(
    bom: &mut Value,
    metadata: &Metadata,
    sources: &[Source],
    commit: &str,
    filename: &str,
    checksum: &str,
) -> Result<()> {
    let mut library = bom["metadata"]["component"].take();
    let library_id = library["bom-ref"]
        .as_str()
        .context("missing library identity")?
        .to_owned();
    let version = library["version"]
        .as_str()
        .context("missing library version")?
        .to_owned();
    let library_ref = format!("pkg:generic/apache/asyncband@{commit}#asyncband");
    library["purl"] = json!(library_ref);
    library
        .as_object_mut()
        .context("invalid library component")?
        .remove("components");
    let mut components = bom["components"].as_array().cloned().unwrap_or_default();
    components.push(library);
    let mut refs = BTreeMap::new();
    for component in &mut components {
        let id = component["bom-ref"]
            .as_str()
            .context("missing component identity")?
            .to_owned();
        let bundled = id == library_id;
        ensure!(
            bundled || id.starts_with(CRATES_IO),
            "review the supplier for dependency {id}"
        );
        let reference = component["purl"]
            .as_str()
            .context("missing component PURL")?
            .to_owned();
        component["bom-ref"] = json!(reference);
        component["supplier"] = if bundled {
            asf()
        } else {
            json!({"name": "crates.io", "url": ["https://crates.io/"]})
        };
        let properties = component
            .as_object_mut()
            .unwrap()
            .entry("properties")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .context("invalid component properties")?;
        properties.push(property(
            "distribution",
            if bundled { "bundled" } else { "external" },
        ));
        refs.insert(id, reference);
    }
    let mut edges = BTreeMap::<String, BTreeSet<String>>::new();
    for edge in bom["dependencies"]
        .as_array()
        .context("missing library dependency graph")?
    {
        let reference = refs
            .get(
                edge["ref"]
                    .as_str()
                    .context("missing dependency identity")?,
            )
            .context("unknown dependency identity")?;
        let dependencies = edge["dependsOn"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|dependency| {
                refs.get(dependency.as_str().context("invalid dependency identity")?)
                    .cloned()
                    .context("unresolved dependency")
            })
            .collect::<Result<_>>()?;
        edges.insert(reference.clone(), dependencies);
    }
    // Retain Cargo's edge kinds and target conditions only for the graph selected
    // by cargo-cyclonedx; workspace dev-dependencies must not add components.
    for node in &metadata
        .resolve
        .as_ref()
        .context("missing Cargo graph")?
        .nodes
    {
        let Some(reference) = refs.get(&node.id.repr) else {
            continue;
        };
        let component = components
            .iter_mut()
            .find(|c| c["bom-ref"] == *reference)
            .unwrap();
        for dependency in &node.deps {
            let Some(target) = refs.get(&dependency.pkg.repr) else {
                continue;
            };
            if !edges[reference].contains(target) {
                continue;
            }
            for kind in &dependency.dep_kinds {
                let kind_name = match kind.kind {
                    DependencyKind::Normal => "normal",
                    DependencyKind::Build => "build",
                    _ => continue,
                };
                let detail = json!({"ref": target, "kind": kind_name, "target": kind.target.as_ref().map(ToString::to_string)});
                component["properties"]
                    .as_array_mut()
                    .unwrap()
                    .push(property("cargo-dependency", detail));
            }
        }
    }
    let archive_ref = format!("urn:asyncband:source:{commit}");
    let mut bundled = BTreeSet::from([library_ref]);
    for (index, source) in sources.iter().enumerate() {
        // A local file may incorporate several upstream works, and one upstream
        // version may have several separately documented sets of portions.
        let reference = format!("urn:asyncband:source-portions:{index}");
        let license = if source.license == "Apache-2.0" {
            source.license.clone()
        } else {
            format!("Apache-2.0 AND ({})", source.license)
        };
        components.push(json!({
            "type": "file", "bom-ref": reference,
            "name": format!("{} {} derived source", source.name, source.version),
            "version": version, "supplier": asf(), "licenses": [{"expression": license}],
            "properties": [property("distribution", "bundled-derived-source")],
            "evidence": {"occurrences": source.files.keys().map(|path| json!({"location": path})).collect::<Vec<_>>()},
            "pedigree": {
                "notes": "Adapted portions only. LICENSE records exact file mappings and modifications; the complete upstream crate is not bundled.",
                "ancestors": [{
                    "type": "library", "name": source.name, "version": source.version,
                    "purl": format!("pkg:cargo/{}@{}", source.name, source.version),
                    "licenses": [{"expression": source.license}],
                    "externalReferences": source.upstream.iter().map(|url| json!({"type": "vcs", "url": url})).collect::<Vec<_>>()
                }]
            }
        }));
        edges.insert(reference.clone(), BTreeSet::new());
        bundled.insert(reference);
    }
    edges.insert(archive_ref.clone(), bundled);
    let mut identities: BTreeSet<_> = components
        .iter()
        .map(|c| c["bom-ref"].as_str().unwrap().to_owned())
        .collect();
    ensure!(
        identities.len() == components.len(),
        "duplicate component identities"
    );
    identities.insert(archive_ref.clone());
    ensure!(
        edges.keys().cloned().collect::<BTreeSet<_>>() == identities
            && edges.values().all(|targets| targets.is_subset(&identities)),
        "incomplete SBOM dependency graph"
    );
    for component in &mut components {
        component["properties"]
            .as_array_mut()
            .unwrap()
            .sort_by_key(Value::to_string);
    }
    components.sort_by_key(|c| c["bom-ref"].as_str().unwrap().to_owned());
    bom["metadata"]["component"] = json!({
        "type": "file", "bom-ref": archive_ref, "name": filename, "version": version,
        "supplier": asf(), "hashes": [{"alg": "SHA-512", "content": checksum}],
        "externalReferences": [{"type": "vcs", "url": format!("https://github.com/apache/asyncband/tree/{commit}")}]
    });
    bom["metadata"]["supplier"] = asf();
    bom["metadata"]["authors"] =
        json!([{"name": "Apache Asyncband", "email": "dev@asyncband.apache.org"}]);
    bom["metadata"]["lifecycles"] = json!([{"phase": "pre-build"}]);
    bom["metadata"]["tools"].as_array_mut().context("missing generator identity")?.push(json!({"vendor": "The Apache Software Foundation", "name": "Asyncband source SBOM", "version": commit}));
    bom["metadata"]["properties"].as_array_mut().context("missing generator properties")?.push(property("scope", "Published asyncband library normal/build dependencies across all features and targets, plus all incorporated source documented in LICENSE (including tests). External dev/example/benchmark/xtask dependencies, host tools, and system libraries are outside this inventory."));
    bom["components"] = json!(components);
    bom["dependencies"] = edges
        .into_iter()
        .map(|(reference, dependencies)| json!({"ref": reference, "dependsOn": dependencies}))
        .collect();
    Ok(())
}
