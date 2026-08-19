"""fprime-yamcs-runtime: a jlink-trimmed Java runtime for fprime-yamcs

The `java-runtime` directory is populated at wheel-build time by `build_java_runtime.py`,
which trims an Eclipse Temurin JDK to the modules YAMCS requires.
"""
import os
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Tuple

# Root of the trimmed Java runtime image
JAVA_HOME = Path(__file__).resolve().parent / "java-runtime"
# The java executable within the runtime image
JAVA = JAVA_HOME / "bin" / ("java.exe" if os.name == "nt" else "java")


def _java_version() -> Tuple[int, ...]:
    """The packaged JDK version: the first three numbers of the package version"""
    try:
        return tuple(int(number) for number in version("fprime-yamcs-runtime").split(".")[:3])
    except PackageNotFoundError:
        return ()


JAVA_VERSION = _java_version()
