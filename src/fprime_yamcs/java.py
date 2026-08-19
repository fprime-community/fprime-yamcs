""" F Prime YAMCS: Java runtime and classpath resolution

Locates a Java runtime and assembles the YAMCS classpath from pip-installed packages, so that
`fprime-yamcs` runs without requiring Maven or a system JDK.

@author LeStarch

Copyright 2026 LeStarch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import re
import shutil
import subprocess
import sys
from importlib.metadata import entry_points
from pathlib import Path
from typing import Iterable, List, Optional

# YAMCS 5.12 requires Java 17 or later
MINIMUM_JAVA_VERSION = 17
# Entry point group whose entries resolve to YAMCS plugin jar paths
PLUGIN_JAR_ENTRY_POINT_GROUP = "fprime_yamcs.plugin_jars"
# Entry point group whose entries resolve to yamcs-web extension directories
WEB_EXTENSION_ENTRY_POINT_GROUP = "fprime_yamcs.web_extensions"
# Directory within this package holding the prebuilt fprime-yamcs plugin jar
PACKAGED_JARS_DIRECTORY = Path(__file__).resolve().parent / "jars"


class JavaResolutionException(Exception):
    """Raised when the Java runtime or the pip-provided jars cannot be located

    `launch_yamcs` catches this exception to select the Maven fallback, so it covers every
    condition preventing a direct pip-only launch (runtime and classpath alike).
    """


def java_major_version(java: Path) -> Optional[int]:
    """ Determine the major version of the supplied java executable

    Runs `java -version` and parses the version string. Returns None when the executable cannot
    be run or the version cannot be parsed.

    Args:
        java: path to a java executable
    Returns:
        The major Java version, or None when it cannot be determined
    """
    try:
        process = subprocess.run(
            [str(java), "-version"], capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = re.search(r'version "(\d+)(?:\.(\d+))?', process.stderr + process.stdout)
    if match is None:
        return None
    major = int(match.group(1))
    # Pre-9 JDKs report versions as 1.<major> (e.g. 1.8)
    if major == 1 and match.group(2) is not None:
        major = int(match.group(2))
    return major


def find_java() -> Path:
    """ Locate a suitable Java runtime

    Resolution order: JAVA_HOME, then `java` on the PATH, then the pip-installed
    fprime-yamcs-runtime package. Candidates below MINIMUM_JAVA_VERSION are skipped.

    Returns:
        The path to a java executable of at least MINIMUM_JAVA_VERSION
    """
    candidates: List[Path] = []
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidates.append(Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java"))
    path_java = shutil.which("java")
    if path_java:
        candidates.append(Path(path_java))
    for candidate in candidates:
        if not candidate.is_file():
            print(f"[WARNING] Skipping {candidate}: does not exist", file=sys.stderr)
            continue
        version = java_major_version(candidate)
        if version is not None and version >= MINIMUM_JAVA_VERSION:
            return candidate
        reason = (
            "could not determine the Java version" if version is None
            else f"Java {version} is older than the required Java {MINIMUM_JAVA_VERSION}"
        )
        print(f"[WARNING] Skipping {candidate}: {reason}", file=sys.stderr)
    try:
        from fprime_yamcs_runtime import JAVA  # type: ignore[import-not-found]
        if Path(JAVA).is_file():
            return Path(JAVA)
    except ImportError:
        pass
    raise JavaResolutionException(
        f"No Java {MINIMUM_JAVA_VERSION}+ runtime found. Set JAVA_HOME, add java to the "
        "PATH, or `pip install fprime-yamcs-runtime`."
    )


def yamcs_lib_jars() -> Optional[List[Path]]:
    """ Locate the YAMCS dependency jars from the pip-installed fprime-yamcs-bundle package

    Returns:
        A list of jar paths, or None when the bundle package is not installed
    """
    try:
        from fprime_yamcs_bundle import LIB_DIR  # type: ignore[import-not-found]
    except ImportError:
        return None
    jars = sorted(Path(LIB_DIR).glob("*.jar"))
    return jars if jars else None


def packaged_plugin_jars() -> List[Path]:
    """ Locate the prebuilt fprime-yamcs plugin jar shipped with this package

    Returns:
        A list of jar paths found in the packaged jars directory (empty for source checkouts
        where the jar has not been built)
    """
    if not PACKAGED_JARS_DIRECTORY.is_dir():
        return []
    return sorted(PACKAGED_JARS_DIRECTORY.glob("*.jar"))


def _entry_point_paths(group: str) -> List[Path]:
    """ Resolve an entry point group to a list of paths

    Each entry point may load to a path, a string, an iterable of either, or a zero-argument
    callable returning any of those.

    Args:
        group: the entry point group to resolve
    Returns:
        A list of resolved paths
    """
    try:
        selected = entry_points(group=group)
    except TypeError:
        # Python < 3.10: entry_points() returns a dict of lists
        selected = entry_points().get(group, [])
    paths: List[Path] = []
    for entry in selected:
        try:
            loaded = entry.load()
            if callable(loaded):
                loaded = loaded()
            values: Iterable = [loaded] if isinstance(loaded, (str, Path)) else loaded
            resolved_values = [Path(value) for value in values]
        except Exception as exc:
            print(f"[WARNING] Failed to load entry point {entry.name} ({group}): {exc}",
                  file=sys.stderr)
            continue
        for resolved in resolved_values:
            if not resolved.exists():
                print(f"[WARNING] Entry point {entry.name} ({group}) resolved to missing path "
                      f"{resolved}. Skipping.", file=sys.stderr)
                continue
            paths.append(resolved)
    return paths


def _expand_jars(paths: Iterable[Path]) -> List[Path]:
    """ Expand jar paths: directories become their contained *.jar files """
    jars: List[Path] = []
    for path in paths:
        jars.extend(sorted(path.glob("*.jar")) if path.is_dir() else [path])
    return jars


def discovered_plugin_jars() -> List[Path]:
    """ YAMCS plugin jars advertised by installed packages via entry points """
    return _expand_jars(_entry_point_paths(PLUGIN_JAR_ENTRY_POINT_GROUP))


def discovered_web_extension_dirs() -> List[Path]:
    """ yamcs-web extension directories advertised by installed packages via entry points """
    return [path for path in _entry_point_paths(WEB_EXTENSION_ENTRY_POINT_GROUP) if path.is_dir()]


def expand_jar_arguments(jar_arguments: Iterable[Path]) -> List[Path]:
    """ Expand user-supplied --yamcs-plugin-jars values: directories become their *.jar files """
    return _expand_jars(jar_arguments)


def yamcs_launch_command(java: Path, classpath: str, etc_dir: Path, data_dir: Path,
                         jvm_args: Iterable[str] = ()) -> List[str]:
    """ Assemble the direct-launch YAMCS command line

    Args:
        java: path to the java executable
        classpath: the YAMCS classpath (from build_classpath)
        etc_dir: the YAMCS etc configuration directory
        data_dir: the YAMCS data directory
        jvm_args: additional JVM arguments
    Returns:
        The command as a list of arguments
    """
    return [
        str(java), *jvm_args,
        "-Djava.util.logging.manager=org.yamcs.logging.YamcsLogManager",
        "-cp", classpath, "org.yamcs.YamcsServer",
        "--etc-dir", str(etc_dir), "--data-dir", str(data_dir),
    ]


def build_classpath(extra_jars: Iterable[Path]) -> str:
    """ Assemble the YAMCS JVM classpath

    Combines the YAMCS dependency jars (fprime-yamcs-bundle), the packaged fprime-yamcs plugin
    jar, entry-point discovered plugin jars, and any extra jars supplied by the user.

    Args:
        extra_jars: additional jars to append (from --yamcs-plugin-jars)
    Returns:
        A classpath string suitable for `java -cp`
    """
    lib_jars = yamcs_lib_jars()
    if lib_jars is None:
        raise JavaResolutionException(
            "The YAMCS jars are not installed. Run `pip install fprime-yamcs-bundle`."
        )
    plugin_jars = packaged_plugin_jars()
    if not plugin_jars:
        raise JavaResolutionException(
            "The fprime-yamcs plugin jar is missing. Source checkouts must build it first "
            f"(scripts/build-jars.sh populates {PACKAGED_JARS_DIRECTORY})."
        )
    discovered = discovered_plugin_jars()
    for jar in discovered:
        print(f"[INFO] Discovered YAMCS plugin jar (entry point): {jar}", file=sys.stderr)
    entries = lib_jars + plugin_jars + discovered + list(extra_jars)
    return os.pathsep.join(str(entry) for entry in entries)
