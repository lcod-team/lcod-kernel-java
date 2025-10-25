# lcod-kernel-java

Java implementation of the LCOD runtime. The current milestone focuses on shipping a `java -jar` compatible version of `lcod-run` while exposing a lightweight API that can be embedded inside existing JVM applications.

## Requirements

- JDK 21 (the build enforces the toolchain via Gradle; set `JAVA_HOME` or `ORG_GRADLE_JAVA_INSTALLATIONS_PATHS` if your default JDK is newer).
- Gradle Wrapper (`./gradlew`) – already included, no global installation required.

## Building the CLI Jar

```bash
./gradlew clean shadowJar
```

The runnable fat-jar is produced under `build/libs/lcod-run-<version>.jar`. Run it just like the Rust CLI:

```bash
java -jar build/libs/lcod-run-0.1.0-SNAPSHOT.jar \
  --compose examples/flow/foreach_demo/compose.yaml \
  --input tests/payload.json
```

All flags documented in `lcod-spec/docs/lcod-run-cli.md` are wired and currently feed the Java runner skeleton. As the kernel matures, the same entry point will execute real LCOD composes.

## Embedding the Runner

Applications can use the public API directly:

```java
var configuration = LcodRunConfiguration.builder()
    .composeTarget(ComposeTarget.forLocal(Path.of("./compose.yaml")))
    .workingDirectory(Path.of("."))
    .lockFile(Path.of("./lcp.lock"))
    .cacheDirectory(Path.of("./.lcod/cache"))
    .logLevel(LogLevel.INFO)
    .build();

RunResult result = new LcodRunner().run(configuration);
```

The `RunResult` object exposes the execution status, timestamps, metadata, and a helper to serialize the payload as JSON for logging or HTTP responses.

> Current limitation: the bootstrap registry only exposes `lcod://impl/set@1` and a kernel log helper. Upcoming work will register the flow primitives, tooling helpers, and resolver bindings so the Java kernel can run the spec fixtures like the Node/Rust runtimes.
