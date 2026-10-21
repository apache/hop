#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Warn when one library lands in the Hop client assembly at two or more versions.

Reads the client zip (or an unpacked assembly directory). Maven coordinates
from the jar's own pom.properties are used when they match the filename.
Otherwise the filename is the identity. A classifier such as osx-aarch_64 is
the same version. Jars nested inside other jars are left alone.

  tools/check-assembly-jar-versions.py
  tools/check-assembly-jar-versions.py path/to/hop-client.zip
  tools/check-assembly-jar-versions.py --self-test

Exit 0 when every library has a single version, 1 when a mismatch is found,
2 when the assembly path is missing.

Same-version copies in several plugins are reported by tools/find_duplicate_jars.sh.
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import tempfile
import zipfile
from collections import defaultdict
from pathlib import Path

# A hyphen-separated token that is still part of the Maven version.
# "4.2.17.Final" stays one token because the qualifier is joined with a dot.
QUALIFIER = re.compile(
    r"^(snapshot|alpha|beta|rc|cr|final|ga|sp|release|sec)\d*$",
    re.IGNORECASE,
)

SECTION_SAME = "same classloader"
SECTION_OVERRIDE = "plugin overrides lib/core"
SECTION_PLUGINS = "across plugins"
SECTION_ORDER = (SECTION_SAME, SECTION_OVERRIDE, SECTION_PLUGINS)
SECTION_NOTES = {
    SECTION_SAME: "More than one version shares a folder, so classpath order picks a winner.",
    SECTION_OVERRIDE: "The plugin classloader loads its own copy before lib/core.",
    SECTION_PLUGINS: "Each plugin loads its own copy.",
}


def parse_jar_name(filename: str) -> tuple[str, str, str | None] | None:
    """Return (artifact_id, version, classifier) for a Maven jar name.

    Jars with no version in the name, such as the renamed swt.jar copies,
    return None.
    """
    name = filename.rsplit("/", 1)[-1]
    if name.lower().endswith(".jar"):
        name = name[:-4]
    parts = name.split("-")
    for index in range(1, len(parts)):
        if not parts[index] or not parts[index][0].isdigit():
            continue
        end = index + 1
        while end < len(parts) and (QUALIFIER.match(parts[end]) or parts[end].isdigit()):
            end += 1
        artifact = "-".join(parts[:index])
        if not artifact:
            return None
        version = "-".join(parts[index:end])
        classifier = "-".join(parts[end:]) or None
        return artifact, version, classifier
    return None


def parse_properties(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def shaded_group(jar_bytes: bytes) -> str | None:
    """Group id for a relocated Graal jar, which ships no Maven coordinates.

    org.graalvm.shadowed:icu4j and org.graalvm.shadowed:xz are named like the
    upstream jars, but their classes live under org.graalvm.shadowed.
    """
    try:
        jar = zipfile.ZipFile(io.BytesIO(jar_bytes))
    except zipfile.BadZipFile:
        return None
    prefix = "org/graalvm/shadowed/"
    with jar:
        for name in jar.namelist():
            if name.startswith(prefix) and name.endswith(".class"):
                return "org.graalvm.shadowed"
    return None


def read_coordinates(jar_bytes: bytes, artifact_id: str) -> tuple[str, str] | None:
    """Return (groupId, version) for the pom that belongs to this jar.

    Other pom.properties shipped inside an uber jar are ignored. Those name
    libraries that were shaded in, not a separate jar on the classpath.
    """
    try:
        jar = zipfile.ZipFile(io.BytesIO(jar_bytes))
    except zipfile.BadZipFile:
        return None
    with jar:
        match: tuple[str, str] | None = None
        for entry in jar.namelist():
            normalized = entry.replace("\\", "/")
            prefix = "META-INF/maven/"
            if not normalized.startswith(prefix) or not normalized.endswith(
                "/pom.properties"
            ):
                continue
            segments = normalized[len(prefix) :].split("/")
            if len(segments) != 3 or segments[1] != artifact_id:
                continue
            if segments[2] != "pom.properties":
                continue
            props = parse_properties(jar.read(entry).decode("utf-8", "replace"))
            declared = props.get("artifactId", artifact_id)
            group = props.get("groupId", "")
            version = props.get("version", "")
            if declared != artifact_id or not group or not version:
                continue
            match = (group, version)
        return match


def display_path(path: str) -> str:
    normalized = path.replace("\\", "/").lstrip("./")
    if normalized.startswith("hop/"):
        normalized = normalized[4:]
    return normalized


def place_of(path: str) -> str:
    """Classloader folder for a path relative to the hop/ base directory."""
    if path.startswith("lib/core/") or path == "lib/core":
        return "lib/core"
    if path.startswith("lib/jdbc/"):
        return "lib/jdbc"
    if path.startswith("lib/swt/"):
        return "lib/swt"
    parts = path.split("/")
    if len(parts) >= 3 and parts[0] == "plugins":
        return "/".join(parts[:3])
    if "/" in path:
        return path.rsplit("/", 1)[0]
    return "."


def version_sort_key(version: str) -> tuple:
    key = []
    for piece in re.split(r"[.\-]", version):
        if piece.isdigit():
            key.append((0, int(piece), ""))
        else:
            key.append((1, 0, piece.lower()))
    return tuple(key)


class Library:
    def __init__(self, key: str) -> None:
        self.key = key
        # version -> list of (path, place)
        self.locations: dict[str, list[tuple[str, str]]] = defaultdict(list)

    def add(self, version: str, path: str, place: str) -> None:
        self.locations[version].append((path, place))

    @property
    def versions(self) -> list[str]:
        return sorted(self.locations, key=version_sort_key)

    def section(self) -> str:
        by_place: dict[str, set[str]] = defaultdict(set)
        for version, locs in self.locations.items():
            for _path, place in locs:
                by_place[place].add(version)
        if any(len(versions) > 1 for versions in by_place.values()):
            return SECTION_SAME
        places = set(by_place)
        plugins = {place for place in places if place.startswith("plugins/")}
        if "lib/core" in places and (plugins or "lib/jdbc" in places):
            return SECTION_OVERRIDE
        return SECTION_PLUGINS


def collect(entries) -> tuple[dict[str, Library], int]:
    """Group jar entries. Each entry is (path inside the assembly, jar bytes)."""
    identified: dict[tuple[str, str], Library] = {}
    unidentified: dict[str, Library] = {}
    jar_count = 0

    for raw_path, payload in entries:
        jar_count += 1
        filename = raw_path.replace("\\", "/").rsplit("/", 1)[-1]
        parsed = parse_jar_name(filename)
        if parsed is None:
            continue
        artifact, filename_version, _classifier = parsed
        path = display_path(raw_path)
        place = place_of(path)
        coordinates = read_coordinates(payload, artifact)
        if coordinates is None:
            shaded = shaded_group(payload)
            key = f"{shaded}:{artifact}" if shaded else artifact
            library = unidentified.get(key)
            if library is None:
                library = Library(key)
                unidentified[key] = library
            library.add(filename_version, path, place)
            continue
        group, version = coordinates
        key = (group, artifact)
        library = identified.get(key)
        if library is None:
            library = Library(f"{group}:{artifact}")
            identified[key] = library
        library.add(version, path, place)

    # A jar with no Maven coordinates stays on its filename. Graal's shaded
    # icu4j jar is named icu4j-<version>.jar and has no pom.properties, but its
    # packages live under org.graalvm.shadowed and it is not com.ibm.icu:icu4j.
    merged = {lib.key: lib for lib in identified.values()}
    for library in unidentified.values():
        merged[library.key] = library
    return merged, jar_count


def mismatches(libraries: dict[str, Library]) -> list[Library]:
    found = [library for library in libraries.values() if len(library.versions) > 1]
    found.sort(key=lambda library: (SECTION_ORDER.index(library.section()), library.key))
    return found


def iter_entries(source: Path):
    if source.is_dir():
        for path in sorted(source.rglob("*.jar")):
            if path.is_file():
                yield path.relative_to(source).as_posix(), path.read_bytes()
        return
    with zipfile.ZipFile(source) as archive:
        for info in archive.infolist():
            name = info.filename
            if name.endswith("/") or not name.endswith(".jar"):
                continue
            yield name, archive.read(info)


def format_report(label: str, libraries: dict[str, Library], jar_count: int) -> str:
    found = mismatches(libraries)
    if not found:
        return f"No version mismatches in {label} ({jar_count} jars).\n"
    lines = [f"Version mismatches in {label}", ""]
    current = None
    for library in found:
        section = library.section()
        if section != current:
            lines.append(section)
            lines.append(f"  {SECTION_NOTES[section]}")
            current = section
        lines.append(f"  {library.key}")
        width = max(len(version) for version in library.versions)
        for version in library.versions:
            paths = sorted(path for path, _place in library.locations[version])
            for index, path in enumerate(paths):
                shown = version if index == 0 else ""
                lines.append(f"    {shown:<{width}}  {path}")
        lines.append("")
    count = len(found)
    noun = "library" if count == 1 else "libraries"
    lines.append(f"{count} {noun} with more than one version.")
    lines.append(
        "Same-version copies are reported by tools/find_duplicate_jars.sh."
    )
    return "\n".join(lines) + "\n"


def default_client_zip() -> Path | None:
    root = Path(__file__).resolve().parent.parent
    candidates = list(root.glob("assemblies/client/target/hop-client-*.zip"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def friendly(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return str(path)


def scan_source(source: Path) -> tuple[str, int]:
    libraries, jar_count = collect(iter_entries(source))
    report = format_report(friendly(source), libraries, jar_count)
    return report, len(mismatches(libraries))


def jar_bytes(
    poms: list[tuple[str, str, str]], extra: dict[str, bytes] | None = None
) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        for group, artifact, version in poms:
            archive.writestr(
                f"META-INF/maven/{group}/{artifact}/pom.properties",
                f"groupId={group}\nartifactId={artifact}\nversion={version}\n",
            )
        for name, payload in (extra or {}).items():
            archive.writestr(name, payload)
    return buffer.getvalue()


def self_test() -> int:
    errors: list[str] = []

    def check_parse(filename: str, expected) -> None:
        got = parse_jar_name(filename)
        if got != expected:
            errors.append(f"parse {filename}: {got!r} != {expected!r}")

    check_parse("zstd-jni-1.5.7-13.jar", ("zstd-jni", "1.5.7-13", None))
    check_parse(
        "netty-resolver-dns-native-macos-4.2.17.Final-osx-aarch_64.jar",
        ("netty-resolver-dns-native-macos", "4.2.17.Final", "osx-aarch_64"),
    )
    check_parse(
        "hop-action-abort-2.20.0-SNAPSHOT.jar",
        ("hop-action-abort", "2.20.0-SNAPSHOT", None),
    )
    check_parse("swt.jar", None)
    check_parse(
        "opentelemetry-semconv-1.29.0-alpha.jar",
        ("opentelemetry-semconv", "1.29.0-alpha", None),
    )
    check_parse("javax.inject-1.jar", ("javax.inject", "1", None))
    check_parse("guava-33.4.0-jre.jar", ("guava", "33.4.0", "jre"))
    check_parse("log4j-api-2.24.3.jar", ("log4j-api", "2.24.3", None))

    entries: list[tuple[str, bytes]] = [
        ("hop/lib/core/widget-1.0.jar", jar_bytes([("com.example", "widget", "1.0")])),
        ("hop/lib/core/widget-2.0.jar", jar_bytes([("com.example", "widget", "2.0")])),
        ("hop/lib/core/commons-io-2.16.0.jar", jar_bytes([])),
        ("hop/plugins/transforms/rest/lib/commons-io-2.16.0.jar", jar_bytes([])),
        (
            "hop/lib/core/netty-resolver-dns-native-macos-4.2.17.Final-osx-aarch_64.jar",
            jar_bytes([]),
        ),
        (
            "hop/lib/core/netty-resolver-dns-native-macos-4.2.17.Final-osx-x86_64.jar",
            jar_bytes([]),
        ),
        (
            "hop/lib/core/annotations-2.20.162.jar",
            jar_bytes([("software.amazon.awssdk", "annotations", "2.20.162")]),
        ),
        (
            "hop/lib/core/annotations-4.1.1.4.jar",
            jar_bytes([("com.google.android", "annotations", "4.1.1.4")]),
        ),
        ("hop/lib/core/annotations-26.1.0.jar", jar_bytes([])),
        (
            "hop/plugins/transforms/languagemodelchat/lib/annotations-26.1.0.jar",
            jar_bytes([]),
        ),
        ("hop/plugins/transforms/fake/lib/api-common-1.0.jar", jar_bytes([])),
        ("hop/plugins/transforms/rest/lib/api-common-2.0.jar", jar_bytes([])),
        (
            "hop/lib/core/icu4j-78.1.jar",
            jar_bytes([("com.ibm.icu", "icu4j", "78.1")]),
        ),
        ("hop/plugins/transforms/script/lib/icu4j-25.0.3.jar", jar_bytes([])),
        (
            "hop/lib/core/webservices-api-4.0.5.jar",
            jar_bytes(
                [
                    ("org.glassfish.metro", "webservices-api", "4.0.5"),
                    ("jakarta.activation", "jakarta.activation-api", "2.1.3"),
                ]
            ),
        ),
        (
            "hop/lib/core/jakarta.activation-api-2.1.4.jar",
            jar_bytes([("jakarta.activation", "jakarta.activation-api", "2.1.4")]),
        ),
        ("hop/lib/swt/linux/x86_64/swt.jar", jar_bytes([])),
        ("hop/lib/swt/osx/arm64/swt.jar", jar_bytes([])),
        ("hop/lib/core/xz-1.9.jar", jar_bytes([])),
        (
            "hop/plugins/transforms/script/lib/xz-25.0.3.jar",
            jar_bytes([], {"org/graalvm/shadowed/org/tukaani/xz/Foo.class": b"class"}),
        ),
    ]

    def check_assembly(source_entries, label: str) -> None:
        libraries, _count = collect(source_entries)
        found = {library.key: library for library in mismatches(libraries)}
        expected = {
            "com.example:widget": SECTION_SAME,
            "api-common": SECTION_PLUGINS,
        }
        if set(found) != set(expected):
            errors.append(f"{label} keys {sorted(found)} != {sorted(expected)}")
        for key, section in expected.items():
            library = found.get(key)
            if library is not None and library.section() != section:
                errors.append(f"{label} {key} section {library.section()} != {section}")
        if "com.ibm.icu:icu4j" in found or "icu4j" in found or "xz" in found:
            errors.append(f"{label} shaded jars were reported as {sorted(found)}")

    check_assembly(entries, "entries")

    with tempfile.TemporaryDirectory(prefix="hop-jar-versions-") as tmp:
        root = Path(tmp)
        archive_path = root / "hop-client.zip"
        with zipfile.ZipFile(archive_path, "w") as archive:
            for name, payload in entries:
                archive.writestr(name, payload)
        zipped, _count = collect(iter_entries(archive_path))
        direct, _count = collect(entries)
        zipped_keys = {library.key: library.versions for library in mismatches(zipped)}
        direct_keys = {library.key: library.versions for library in mismatches(direct)}
        if zipped_keys != direct_keys:
            errors.append(f"zip scan {zipped_keys} != direct {direct_keys}")

        unpacked = root / "hop"
        for name, payload in entries:
            path = unpacked / name[len("hop/") :]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        from_dir, _count = collect(iter_entries(unpacked))
        dir_keys = {library.key: library.versions for library in mismatches(from_dir)}
        if dir_keys != direct_keys:
            errors.append(f"directory scan {dir_keys} != direct {direct_keys}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("self-test passed")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "assembly",
        nargs="?",
        help="Client assembly zip or unpacked directory. "
        "Default: newest assemblies/client/target/hop-client-*.zip",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Build a tiny assembly and check the verdicts",
    )
    args = parser.parse_args(argv)
    if args.self_test:
        if args.assembly:
            print("ERROR: --self-test takes no assembly path", file=sys.stderr)
            return 2
        return self_test()

    if args.assembly:
        source = Path(args.assembly)
        if not source.exists():
            print(f"ERROR: assembly not found: {args.assembly}", file=sys.stderr)
            return 2
    else:
        source = default_client_zip()
        if source is None:
            print(
                "ERROR: no client assembly zip found under assemblies/client/target. "
                "Pass the zip path, or package the client assembly.",
                file=sys.stderr,
            )
            return 2

    try:
        report, mismatch_count = scan_source(source)
    except zipfile.BadZipFile:
        print(f"ERROR: not a zip file: {friendly(source)}", file=sys.stderr)
        return 2
    sys.stdout.write(report)
    return 1 if mismatch_count else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
