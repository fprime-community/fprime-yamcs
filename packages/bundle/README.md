# fprime-yamcs-bundle

The [YAMCS](https://yamcs.org/) mission control framework and its dependency jars, packaged
for pip so that [fprime-yamcs](https://github.com/fprime-community/fprime-yamcs) runs without
Maven. Installed automatically as a dependency of `fprime-yamcs`.

The jars are resolved by Maven at wheel-build time from the fprime-yamcs pom
(`mvn dependency:copy-dependencies`) and exposed to Python as `fprime_yamcs_bundle.LIB_DIR`.

The wheel version's first three numbers track the packaged YAMCS version; the fourth is the
repackaging number.

YAMCS is licensed under the AGPL-3.0, and so is this package. The fprime-yamcs Python code and
its YAMCS plugin jar are distributed separately under Apache-2.0 in the `fprime-yamcs` package.
