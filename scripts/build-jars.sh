#!/usr/bin/env bash
# Build the fprime-yamcs plugin jar and collect the YAMCS dependency jars into the
# fprime-yamcs and fprime-yamcs-bundle package trees. Requires mvn + JDK (build-time only).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
POM="${ROOT}/src/fprime_yamcs/yamcs/pom.xml"
PLUGIN_JAR_DIR="${ROOT}/src/fprime_yamcs/jars"
BUNDLE_LIB_DIR="${ROOT}/packages/bundle/src/fprime_yamcs_bundle/lib"

# -Dyamcs.skip=true skips the yamcs-maven-plugin bundle goal (the tar.gz is not needed)
mvn -B -C -f "${POM}" -Dyamcs.skip=true package

rm -rf "${PLUGIN_JAR_DIR}" "${BUNDLE_LIB_DIR}"
mkdir -p "${PLUGIN_JAR_DIR}" "${BUNDLE_LIB_DIR}"
cp "${ROOT}"/src/fprime_yamcs/yamcs/target/fprime-yamcs-*.jar "${PLUGIN_JAR_DIR}/"

mvn -B -C -f "${POM}" -Dyamcs.skip=true dependency:copy-dependencies \
    -DincludeScope=runtime -DoutputDirectory="${BUNDLE_LIB_DIR}"

echo "[INFO] Plugin jar: $(ls "${PLUGIN_JAR_DIR}")"
echo "[INFO] Bundle jars: $(ls "${BUNDLE_LIB_DIR}" | wc -l) jars in ${BUNDLE_LIB_DIR}"
