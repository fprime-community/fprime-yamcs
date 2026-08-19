"""Build the trimmed Java runtime image for the fprime-yamcs-runtime wheel

Computes the module set YAMCS requires by running jdeps over the YAMCS jars (populated in
../bundle by scripts/build-jars.sh), adds modules loaded reflectively that jdeps cannot see,
and assembles the runtime image with jlink. Requires a JDK (jdeps, jlink) on the PATH — this
runs at wheel-build time in CI, never on user machines.
"""
import argparse
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent / "src" / "fprime_yamcs_runtime"
PYPROJECT = Path(__file__).resolve().parent / "pyproject.toml"
RUNTIME_DIR = PACKAGE_DIR / "java-runtime"
DEFAULT_JARS_DIR = (
    Path(__file__).resolve().parent.parent / "bundle" / "src" / "fprime_yamcs_bundle" / "lib"
)

# Modules jdeps cannot detect (reflective/service loading) plus operability tooling
EXTRA_MODULES = [
    "jdk.crypto.ec",       # TLS elliptic-curve support, service-loaded
    "jdk.jcmd",            # jcmd/jstack/jmap for diagnosing a running server
    "jdk.jfr",             # flight recorder
    "jdk.localedata",      # non-US locale data (trimmed by --include-locales)
    "jdk.management",      # com.sun.management MXBeans used by SystemParametersService
    "jdk.management.agent",
    "jdk.management.jfr",
    "jdk.net",             # extended socket options used by netty
    "jdk.security.auth",
    "jdk.unsupported",     # sun.misc.Unsafe, required by netty and rocksdb
    "jdk.zipfs",
]
INCLUDED_LOCALES = ["en", "en-US"]


def jdk_major_version() -> str:
    """The JDK major version, single-sourced from the runtime package version"""
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)["project"]["version"].split(".")[0]


def compute_modules(jars_dir: Path) -> str:
    """Compute the comma-separated module list for jlink from the YAMCS jars via jdeps"""
    jars = sorted(str(jar) for jar in jars_dir.glob("*.jar"))
    if not jars:
        raise SystemExit(f"No jars found in {jars_dir}. Run scripts/build-jars.sh first.")
    # --multi-release must match the shipped JDK so version-specific classes are analyzed
    process = subprocess.run(
        ["jdeps", "--multi-release", jdk_major_version(), "--ignore-missing-deps",
         "--print-module-deps", "--class-path", str(jars_dir / "*"), *jars],
        capture_output=True, text=True, check=True,
    )
    lines = process.stdout.strip().splitlines()
    if not lines or not re.fullmatch(r"[A-Za-z0-9_.]+(,[A-Za-z0-9_.]+)*", lines[-1]):
        raise SystemExit(
            f"jdeps produced no module list.\nstdout: {process.stdout}\nstderr: {process.stderr}"
        )
    detected = lines[-1].split(",")
    modules = sorted(set(detected) | set(EXTRA_MODULES))
    print(f"[INFO] jlink modules: {','.join(modules)}")
    return ",".join(modules)


def build_java_runtime(jars_dir: Path) -> None:
    """Assemble the trimmed runtime image with jlink"""
    shutil.rmtree(RUNTIME_DIR, ignore_errors=True)
    subprocess.run(
        ["jlink",
         "--add-modules", compute_modules(jars_dir),
         f"--include-locales={','.join(INCLUDED_LOCALES)}",
         "--strip-debug",
         "--compress", "zip-6",
         "--no-man-pages",
         "--no-header-files",
         "--output", str(RUNTIME_DIR)],
        check=True,
    )
    print(f"[INFO] Java runtime image created at {RUNTIME_DIR}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jars-dir", type=Path, default=DEFAULT_JARS_DIR,
                        help="Directory of YAMCS jars to compute the module set from")
    arguments = parser.parse_args()
    try:
        build_java_runtime(arguments.jars_dir)
    except subprocess.CalledProcessError as error:
        print(f"[ERROR] {error.cmd[0]} failed: {error.stderr or error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
