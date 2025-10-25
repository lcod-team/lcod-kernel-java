# Roadmap — lcod-kernel-java

The Java kernel must stay aligned with the LCOD spec roadmap (see `lcod-spec/ROADMAP.md` M3 items). Deliverables below assume Java 21 compatibility and enforce builds/tests via the Gradle Wrapper (`./gradlew`).

## Phase 0 — Bootstrap

- [ ] KJ-01 Tooling baseline: add Gradle wrapper + `build.gradle.kts`, pin the Java toolchain to 21, wire lint/test tasks, and configure CI to run `./gradlew check`.
- [ ] KJ-02 Runtime skeleton: implement the compose runner entry point (CLI + library), expose slot helpers (`runChildren`/`runSlot` equivalents), and stub the axiom bridge so the kernel can execute no-op flows.
- [ ] KJ-03 Documentation: describe project layout, build instructions, and how the Java kernel maps to spec concepts (README + CONTRIBUTING).

## Phase 1 — Conformance

- [ ] KJ-10 Shared fixtures: import `lcod-spec/tests/conformance` assets as fixtures, plus any generated data needed for Java serialization.
- [ ] KJ-11 Resolver integration: ensure the Java runner consumes resolver outputs/lockfiles identically to JS/Rust (paths, env overrides, workspace awareness).
- [ ] KJ-12 Flow coverage: implement foreach/parallel/try operators with streaming + `$slot.*` semantics, matching the Node/Rust reference behaviour.
- [ ] KJ-13 Conformance harness: add a Gradle task (e.g., `./gradlew conformance`) that executes the shared manifest, diffing Java results against the reference JSON, and surface it in CI.
- [ ] KJ-14 CI parity: update workflows to run `npm run conformance` in `lcod-spec`, `cargo test` in Rust, and `./gradlew conformance` so regressions stay visible across runtimes.
