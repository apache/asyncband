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

import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

import sbom


def fixture(root):
    member = f"path+file://{root}/asyncband#0.7.4"
    helper = f"{sbom.CRATES_IO}#helper@1.0.0"
    testing = f"{sbom.CRATES_IO}#testing@2.0.0"
    packages = [
        {
            "id": member,
            "name": "asyncband",
            "version": "0.7.4",
            "source": None,
            "manifest_path": f"{root}/asyncband/Cargo.toml",
            "license": "Apache-2.0",
        },
        {
            "id": helper,
            "name": "helper",
            "version": "1.0.0",
            "source": sbom.CRATES_IO,
            "license": "MIT",
        },
        {
            "id": testing,
            "name": "testing",
            "version": "2.0.0",
            "source": sbom.CRATES_IO,
            "license": "Apache-2.0",
        },
    ]
    metadata = {
        "workspace_root": str(root),
        "workspace_members": [member],
        "packages": packages,
        "resolve": {
            "nodes": [
                {
                    "id": member,
                    "deps": [
                        {
                            "pkg": helper,
                            "dep_kinds": [
                                {"kind": None, "target": None},
                                {"kind": "build", "target": "cfg(unix)"},
                            ],
                        },
                        {
                            "pkg": testing,
                            "dep_kinds": [{"kind": "dev", "target": None}],
                        },
                    ],
                },
                {"id": helper, "deps": []},
                {"id": testing, "deps": []},
            ]
        },
    }
    generated = [
        {
            "metadata": {
                "component": {
                    "bom-ref": member,
                    "type": "library",
                    "purl": "pkg:cargo/asyncband@0.7.4?download_url=file://.",
                    "components": [
                        {"bom-ref": member + " bin-target-0", "name": "asyncband"}
                    ],
                }
            },
            "components": [
                {
                    "bom-ref": helper,
                    "type": "library",
                    "licenses": [{"expression": "MIT"}],
                }
            ],
        }
    ]
    lock = {"package": [dict(package, checksum="1" * 64) for package in packages]}
    work = {
        "name": "ancestor",
        "version": "1.2.3",
        "license": "MIT",
        "upstream": "https://example.org/ancestor/commit/" + "a" * 40,
        "files": ["asyncband/src/lib.rs"],
    }
    return metadata, generated, lock, [work]


def build(root):
    return sbom.assemble(
        *fixture(root),
        "a" * 40,
        "0.7.4",
        1700000000,
        "apache-asyncband-0.7.4-incubating-src.tar.gz",
        "2" * 128,
    )


class SbomTests(unittest.TestCase):
    def test_complete_graph_includes_dev_dependencies_and_source_pedigree(self):
        document = build(Path("/source"))
        sbom.validate(document)
        components = {
            component["name"]: component for component in document["components"]
        }
        main = components["asyncband"]
        edges = {edge["ref"]: edge["dependsOn"] for edge in document["dependencies"]}
        self.assertEqual(
            set(edges[main["bom-ref"]]),
            {
                components["helper"]["bom-ref"],
                components["testing"]["bom-ref"],
                components["ancestor 1.2.3 derived source"]["bom-ref"],
            },
        )
        kinds = [
            json.loads(prop["value"])
            for prop in main["properties"]
            if prop["name"].endswith(":cargo-dependency")
        ]
        self.assertEqual({kind["kind"] for kind in kinds}, {"normal", "build", "dev"})
        self.assertIn(
            {
                "kind": "build",
                "ref": components["helper"]["bom-ref"],
                "target": "cfg(unix)",
            },
            kinds,
        )
        self.assertEqual(
            components["testing"]["hashes"], [{"alg": "SHA-256", "content": "1" * 64}]
        )
        derived = components["ancestor 1.2.3 derived source"]
        self.assertNotIn("purl", derived)
        self.assertEqual(derived["pedigree"]["ancestors"][0]["version"], "1.2.3")
        self.assertEqual(
            derived["evidence"]["occurrences"], [{"location": "asyncband/src/lib.rs"}]
        )
        self.assertEqual(
            document["metadata"]["component"]["hashes"],
            [{"alg": "SHA-512", "content": "2" * 128}],
        )

    def test_output_is_independent_of_extraction_path_and_metadata_order(self):
        first = sbom.encode(build(Path("/first/source")))
        metadata, generated, lock, works = fixture(Path("/a different/source"))
        metadata["packages"].reverse()
        metadata["resolve"]["nodes"].reverse()
        for node in metadata["resolve"]["nodes"]:
            node["deps"].reverse()
        second = sbom.encode(
            sbom.assemble(
                metadata,
                generated,
                lock,
                works,
                "a" * 40,
                "0.7.4",
                1700000000,
                "apache-asyncband-0.7.4-incubating-src.tar.gz",
                "2" * 128,
            )
        )
        self.assertEqual(first, second)
        self.assertNotIn("file://", first)
        self.assertNotIn("/first/source", first)

    def test_graph_validation_rejects_unresolved_dependencies(self):
        document = build(Path("/source"))
        document["dependencies"][0]["dependsOn"].append("missing-component")
        with self.assertRaisesRegex(ValueError, "Dangling"):
            sbom.validate(document)

    def test_new_dependency_source_requires_explicit_support(self):
        metadata, generated, lock, works = fixture(Path("/source"))
        metadata["packages"][-1]["source"] = "git+https://example.org/library"
        with self.assertRaisesRegex(ValueError, "source/supplier support"):
            sbom.assemble(
                metadata,
                generated,
                lock,
                works,
                "a" * 40,
                "0.7.4",
                0,
                "source.tar.gz",
                "2" * 128,
            )

    def test_missing_lock_checksum_is_not_silently_omitted(self):
        metadata, generated, lock, works = fixture(Path("/source"))
        lock["package"][-1].pop("checksum")
        with self.assertRaisesRegex(ValueError, "Cargo.lock checksum"):
            sbom.assemble(
                metadata,
                generated,
                lock,
                works,
                "a" * 40,
                "0.7.4",
                0,
                "source.tar.gz",
                "2" * 128,
            )

    def test_origin_index_tracks_license_and_actual_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "asyncband/src").mkdir(parents=True)
            (root / "asyncband/src/lib.rs").touch()
            (root / "xtask").mkdir()
            (root / "xtask/sbom-sources.toml").write_text("""
[[works]]
name = "ancestor"
version = "1.2.3"
license = "MIT"
upstream = "https://example.org/revision"
files = ["asyncband/src/lib.rs"]
""")
            license_text = (
                "ancestor 1.2.3 https://example.org/revision asyncband/src/lib.rs"
            )
            (root / "LICENSE").write_text(license_text)
            self.assertEqual(len(sbom.read_origins(root, ["asyncband"])), 1)
            (root / "LICENSE").write_text(license_text + " asyncband/src/new.rs")
            with self.assertRaisesRegex(ValueError, "paths differ"):
                sbom.read_origins(root, ["asyncband"])
            (root / "LICENSE").write_text(license_text)
            (root / "asyncband/src/lib.rs").unlink()
            with self.assertRaisesRegex(ValueError, "bundled file"):
                sbom.read_origins(root, ["asyncband"])

    def test_archive_cannot_write_through_an_escaping_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "apache-asyncband-0.7.4-incubating-src.tar.gz"
            prefix = archive.name.removesuffix(".tar.gz")
            with tarfile.open(
                archive,
                "w:gz",
                format=tarfile.PAX_FORMAT,
                pax_headers={"comment": "a" * 40},
            ) as output:
                link = tarfile.TarInfo(prefix + "/escape")
                link.type = tarfile.SYMTYPE
                link.linkname = "../../outside"
                output.addfile(link)
                member = tarfile.TarInfo(prefix + "/escape/file")
                member.size = 4
                output.addfile(member, io.BytesIO(b"data"))
            with self.assertRaises(tarfile.FilterError):
                sbom.extract_source(archive, root / "extract")
            self.assertFalse((root / "outside").exists())
