# fprime-yamcs-runtime

A [jlink](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jlink.html)-trimmed Java
runtime for [fprime-yamcs](https://github.com/fprime-community/fprime-yamcs), built from
[Eclipse Temurin](https://adoptium.net/) with the modules YAMCS requires (computed with jdeps
plus reflectively-loaded additions). Installed automatically as a dependency of
`fprime-yamcs`; used only when no suitable system Java (`JAVA_HOME` or `java` >= 17 on the
PATH) is found.

```python
>>> from fprime_yamcs_runtime import JAVA, JAVA_HOME, JAVA_VERSION
```

The wheel version's first three numbers track the Temurin JDK version; the fourth is the
repackaging number.

OpenJDK (and therefore this runtime image) is licensed GPL-2.0 with the Classpath Exception.
