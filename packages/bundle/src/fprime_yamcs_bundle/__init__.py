"""fprime-yamcs-bundle: the YAMCS jars (AGPL-3.0) consumed by fprime-yamcs

The `lib` directory is populated at wheel-build time with the YAMCS framework jars and their
transitive dependencies, resolved by Maven from the fprime-yamcs pom.
"""
from pathlib import Path

# Directory containing the YAMCS framework jars and their dependencies
LIB_DIR = Path(__file__).resolve().parent / "lib"
