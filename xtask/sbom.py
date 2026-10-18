# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Compose a CycloneDX SBOM from the exact source archive being released."""

import argparse
import copy
import datetime
import hashlib
import json
import os
import re
import subprocess
import tarfile
import tempfile
import tomllib
import unittest
from pathlib import Path
from urllib.parse import quote

GENERATOR_VERSION = "0.5.9"
CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"
ASF = {"name": "The Apache Software Foundation", "url": ["https://www.apache.org/"]}
REPOSITORY = "https://github.com/apache/asyncband"


def property_value(name, value):
    return {"name": f"apache:asyncband:{name}", "value": value}


def encode(document):
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def run(*args, cwd=None, env=None):
    return subprocess.run(
        args, cwd=cwd, env=env, check=True, text=True, stdout=subprocess.PIPE
    ).stdout


def extract_source(archive, destination):
    prefix = archive.name.removesuffix(".tar.gz")
    with tarfile.open(archive, "r:gz") as source:
        commit = source.pax_headers.get("comment", "")
        if not re.fullmatch(r"[0-9a-f]{40}", commit):
            raise ValueError("Expected the commit recorded by git archive")
        members = source.getmembers()
        if not members or any(
            member.name.split("/")[0] != prefix for member in members
        ):
            raise ValueError("Archive must have one root matching its filename")
        source.extractall(destination, filter="data")
        epoch = members[0].mtime
    root = (destination / prefix).resolve()
    version = tomllib.loads((root / "asyncband/Cargo.toml").read_text())["package"][
        "version"
    ]
    if archive.name != f"apache-asyncband-{version}-incubating-src.tar.gz":
        raise ValueError("Source archive name and package version differ")
    return root, commit, version, epoch


def read_origins(root, workspace_paths):
    works = tomllib.loads((root / "xtask/sbom-sources.toml").read_text())["works"]
    license_text = " ".join((root / "LICENSE").read_text().split())
    declared = set()
    identifiers = set()
    for work in works:
        identifier = (work["name"], work["version"])
        if identifier in identifiers:
            raise ValueError(f"Duplicate source origin: {identifier}")
        identifiers.add(identifier)
        if work["upstream"] not in license_text:
            raise ValueError(
                f"Upstream source missing from LICENSE: {work['upstream']}"
            )
        if not re.search(
            rf"\b{re.escape(work['name'])}\s+v?{re.escape(work['version'])}\b",
            license_text,
            re.IGNORECASE,
        ):
            raise ValueError(f"Upstream version missing from LICENSE: {identifier}")
        for filename in work["files"]:
            path = (root / filename).resolve()
            if not path.is_relative_to(root.resolve()) or not path.is_file():
                raise ValueError(
                    f"Source origin does not name a bundled file: {filename}"
                )
            declared.add(filename)
    prefixes = "|".join(re.escape(path) for path in workspace_paths)
    documented = set(re.findall(rf"\b(?:{prefixes})/[\w/.-]+\.[\w]+\b", license_text))
    if declared != documented:
        raise ValueError(
            f"Source origin paths differ from LICENSE: {sorted(declared ^ documented)}"
        )
    return works


def assemble(
    metadata, generated, lock, works, commit, version, epoch, filename, checksum
):
    root = Path(metadata["workspace_root"])
    packages = {package["id"]: package for package in metadata["packages"]}
    members = set(metadata["workspace_members"])
    paths = {
        identifier: Path(packages[identifier]["manifest_path"])
        .parent.relative_to(root)
        .as_posix()
        for identifier in members
    }
    refs = {}
    for identifier, package in packages.items():
        if identifier in members:
            refs[identifier] = f"urn:asyncband:workspace:{paths[identifier]}"
        elif package["source"] == CRATES_IO:
            refs[identifier] = f"pkg:cargo/{package['name']}@{package['version']}"
        else:
            raise ValueError(
                f"Add explicit SBOM source/supplier support for {identifier}"
            )
    if len(set(refs.values())) != len(refs):
        raise ValueError("Cargo packages have conflicting SBOM identifiers")

    # Prefer each workspace member's own BOM to its abbreviated entry in another BOM.
    base = {
        bom["metadata"]["component"]["bom-ref"]: bom["metadata"]["component"]
        for bom in generated
    }
    for bom in generated:
        for component in bom.get("components", []):
            base.setdefault(component["bom-ref"], component)
    hashes = {
        (p["name"], p["version"], p.get("source")): p.get("checksum")
        for p in lock["package"]
    }
    components = {}
    for identifier, package in packages.items():
        component = copy.deepcopy(base.get(identifier, {}))
        # Cargo target subcomponents carry absolute paths and add no source-package inventory.
        for field in ("components", "scope", "properties"):
            component.pop(field, None)
        component.update(
            {
                "bom-ref": refs[identifier],
                "name": package["name"],
                "version": package["version"],
            }
        )
        component.setdefault("type", "library")
        component.setdefault(
            "licenses",
            [{"expression": package["license"]}] if package["license"] else [],
        )
        if not component["licenses"]:
            raise ValueError(f"Review missing license metadata for {identifier}")
        if identifier in members:
            component["supplier"] = ASF
            component["purl"] = (
                f"pkg:generic/apache/asyncband@{commit}#{quote(paths[identifier])}"
            )
            component["properties"] = [
                property_value("distribution", "bundled"),
                property_value("source-path", paths[identifier]),
            ]
        else:
            # Supplier identifies the distribution service, not an inferred author/employer.
            component["supplier"] = {"name": "crates.io", "url": ["https://crates.io/"]}
            component["purl"] = refs[identifier]
            digest = hashes.get(
                (package["name"], package["version"], package["source"])
            )
            if not digest:
                raise ValueError(f"Missing Cargo.lock checksum for {identifier}")
            component["hashes"] = [{"alg": "SHA-256", "content": digest}]
            component["properties"] = [property_value("distribution", "external")]
            if package.get("repository") and not component.get("externalReferences"):
                component["externalReferences"] = [
                    {"type": "vcs", "url": package["repository"]}
                ]
        components[refs[identifier]] = component

    # cargo-cyclonedx omits dev-only dependencies. The locked Cargo graph supplies
    # those packages and preserves edge kinds/target conditions for this source BOM.
    edges = {ref: set() for ref in components}
    for node in metadata["resolve"]["nodes"]:
        ref = refs[node["id"]]
        for dependency in node["deps"]:
            dependency_ref = refs[dependency["pkg"]]
            edges[ref].add(dependency_ref)
            for kind in dependency["dep_kinds"]:
                detail = {
                    "ref": dependency_ref,
                    "kind": kind["kind"] or "normal",
                    "target": kind["target"],
                }
                components[ref]["properties"].append(
                    property_value(
                        "cargo-dependency", json.dumps(detail, sort_keys=True)
                    )
                )

    for work in works:
        ref = f"urn:asyncband:derived:{work['name']}:{work['version']}"
        upstream_license = work["license"]
        license_expression = (
            "Apache-2.0"
            if upstream_license == "Apache-2.0"
            else f"Apache-2.0 AND {upstream_license}"
        )
        components[ref] = {
            "type": "file",
            "bom-ref": ref,
            "name": f"{work['name']} {work['version']} derived source",
            "version": version,
            "supplier": ASF,
            "licenses": [{"expression": license_expression}],
            "properties": [property_value("distribution", "bundled-derived-source")],
            "evidence": {
                "occurrences": [{"location": path} for path in sorted(work["files"])]
            },
            "pedigree": {
                "notes": "Adapted portions only. LICENSE records upstream file mappings and modifications; the complete upstream crate is not bundled.",
                "ancestors": [
                    {
                        "type": "library",
                        "name": work["name"],
                        "version": work["version"],
                        "purl": f"pkg:cargo/{work['name']}@{work['version']}",
                        "licenses": [{"expression": upstream_license}],
                        "externalReferences": [
                            {"type": "vcs", "url": work["upstream"]}
                        ],
                    }
                ],
            },
        }
        edges[ref] = set()
        for filename_in_source in work["files"]:
            owners = [
                identifier
                for identifier, path in paths.items()
                if filename_in_source.startswith(path + "/")
            ]
            if not owners:
                raise ValueError(
                    f"No workspace owner for derived source: {filename_in_source}"
                )
            owner = max(owners, key=lambda identifier: len(paths[identifier]))
            edges[refs[owner]].add(ref)

    archive_ref = f"urn:asyncband:source:{commit}"
    edges[archive_ref] = {refs[identifier] for identifier in members}
    for component in components.values():
        component["properties"].sort(key=lambda item: (item["name"], item["value"]))
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {
            "timestamp": datetime.datetime.fromtimestamp(epoch, datetime.UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "supplier": ASF,
            "authors": [
                {"name": "Apache Asyncband", "email": "dev@asyncband.apache.org"}
            ],
            "tools": [
                {
                    "vendor": "CycloneDX",
                    "name": "cargo-cyclonedx",
                    "version": GENERATOR_VERSION,
                },
                {
                    "vendor": "The Apache Software Foundation",
                    "name": "Asyncband source SBOM",
                    "version": commit,
                },
            ],
            "component": {
                "type": "file",
                "bom-ref": archive_ref,
                "name": filename,
                "version": version,
                "supplier": ASF,
                "hashes": [{"alg": "SHA-512", "content": checksum}],
                "externalReferences": [
                    {"type": "vcs", "url": f"{REPOSITORY}/tree/{commit}"}
                ],
            },
            "properties": [
                property_value(
                    "scope",
                    "Cargo source workspace: all features and target platforms, including build and development dependencies. Registry crates are external, not bundled. GitHub Actions and host tools are outside this inventory.",
                )
            ],
        },
        "components": [components[ref] for ref in sorted(components)],
        "dependencies": [
            {"ref": ref, "dependsOn": sorted(edges[ref])} for ref in sorted(edges)
        ],
    }


def validate(document):
    from cyclonedx.schema import SchemaVersion
    from cyclonedx.validation.json import JsonStrictValidator

    if errors := JsonStrictValidator(SchemaVersion.V1_5).validate_str(
        encode(document), all_errors=True
    ):
        raise ValueError(f"CycloneDX schema validation failed: {errors}")
    refs = {document["metadata"]["component"]["bom-ref"]}
    refs.update(component["bom-ref"] for component in document["components"])
    if {edge["ref"] for edge in document["dependencies"]} != refs:
        raise ValueError("Incomplete SBOM dependency graph")
    if any(
        dependency not in refs
        for edge in document["dependencies"]
        for dependency in edge["dependsOn"]
    ):
        raise ValueError("Dangling SBOM dependency")


def generate(archive, output):
    if run("cargo", "cyclonedx", "--version").split()[-1] != GENERATOR_VERSION:
        raise ValueError(
            f"Install cargo-cyclonedx {GENERATOR_VERSION} with cargo install --locked"
        )
    with archive.open("rb") as file:
        checksum = hashlib.file_digest(file, "sha512").hexdigest()
    with tempfile.TemporaryDirectory(prefix="asyncband-sbom-") as temporary:
        root, commit, version, epoch = extract_source(archive, Path(temporary))
        lock_bytes = (root / "Cargo.lock").read_bytes()
        metadata = json.loads(
            run(
                "cargo",
                "metadata",
                "--locked",
                "--all-features",
                "--format-version",
                "1",
                cwd=root,
            )
        )
        member_paths = [
            Path(package["manifest_path"]).parent
            for package in metadata["packages"]
            if package["id"] in metadata["workspace_members"]
        ]
        works = read_origins(
            root, [path.relative_to(root).as_posix() for path in member_paths]
        )
        env = dict(
            os.environ, SOURCE_DATE_EPOCH=str(int(epoch)), CARGO_NET_OFFLINE="true"
        )
        run(
            "cargo",
            "cyclonedx",
            "--format",
            "json",
            "--spec-version",
            "1.5",
            "--all-features",
            "--target",
            "all",
            cwd=root,
            env=env,
        )
        if (root / "Cargo.lock").read_bytes() != lock_bytes:
            raise ValueError("cargo-cyclonedx changed the release lockfile")
        generated = [
            json.loads(path.with_suffix(".cdx.json").read_text())
            for path in (
                Path(package["manifest_path"]).with_name(package["name"])
                for package in metadata["packages"]
                if package["id"] in metadata["workspace_members"]
            )
        ]
        document = assemble(
            metadata,
            generated,
            tomllib.loads(lock_bytes.decode()),
            works,
            commit,
            version,
            epoch,
            archive.name,
            checksum,
        )
        validate(document)
        encoded = encode(document)
        if str(root) in encoded or "file://" in encoded:
            raise ValueError("SBOM contains a machine-local path")
    output.write_text(encoded, encoding="utf-8", newline="\n")
    print(f"Wrote {output}: {len(document['components'])} components")


def main():
    parser = argparse.ArgumentParser(prog="cargo x sbom")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser(
        "generate", help="Generate a sidecar SBOM from a git source archive"
    )
    build.add_argument("--archive", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    commands.add_parser("test", help="Run SBOM regression tests")
    args = parser.parse_args()
    if args.command == "generate":
        generate(args.archive.resolve(), args.output.resolve())
    else:
        suite = unittest.defaultTestLoader.discover(
            str(Path(__file__).parent), pattern="test_sbom.py"
        )
        if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
            raise SystemExit(1)


if __name__ == "__main__":
    main()
