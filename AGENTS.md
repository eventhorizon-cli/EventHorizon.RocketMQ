# AGENTS.md

You are an AI coding assistant for this repository.

## Scope and working style

- These instructions apply repository-wide unless a more specific `AGENTS.md` exists below the target path.
- Follow [`.editorconfig`](.editorconfig) and nearby code before introducing a new style or abstraction.
- Keep changes focused, maintainable, and production-ready. Preserve unrelated user changes.
- Use the SDK selected by [`global.json`](global.json); do not change the SDK or target-framework policy incidentally.

## References

Use the following as the source of truth for detailed, evolving guidance instead of duplicating it here:

- [Design-note index](docs/README.md): bilingual architecture, protocol, instrumentation, and testing articles.
- [Protocol boundaries and type ownership](docs/en-US/architecture/protocol-boundaries.md): production-project split,
  duplicated models, public API ownership, and folder placement.
- [Dependency-injection registrations and lifetimes](docs/en-US/architecture/dependency-injection-and-lifetimes.md):
  role composition, keyed registrations, consumer engines, handlers, and hosted lifecycles.
- [OpenTelemetry instrumentation](docs/en-US/architecture/opentelemetry-instrumentation.md): trace topology,
  covered operations, semantic attributes, metrics, propagation, and protocol ownership.
- [gRPC consumer model](docs/en-US/grpc/consumer-model.md) and
  [classic Remoting consumer model](docs/en-US/remoting/consumer-model.md), plus
  [classic Remoting transport and roles](docs/en-US/remoting/transport-and-client-roles.md): consumer and transport
  semantics.
- [Local and integration testing](docs/en-US/testing/local-and-integration-testing.md): test projects, Testcontainers,
  CI, coverage, and Compose boundaries.
- [gRPC package user guide](src/EventHorizon.RocketMQ.Grpc/README.md) and
  [classic Remoting package user guide](src/EventHorizon.RocketMQ.Remoting/README.md): installation, public APIs,
  configuration, compatibility prerequisites, and examples.
- [Samples index](samples/README.md) and [test-environment index](test-environments/README.md): runnable local
  setups and their prerequisites.

Read the relevant reference before changing an architectural boundary, transport behavior, DI lifecycle, test
topology, or manual environment.

For every new or changed Producer or Consumer send, receive, processing, retry, acknowledgement, negative
acknowledgement, commit, dead-letter, or lease-renewal path, review the OpenTelemetry design and update the
protocol-owned tracing and metrics together with the behavior and tests. A feature path is not complete when its
corresponding telemetry success, empty-result, cancellation, and failure semantics are missing.

- When an implementation requires a behavioral or design decision, first consult the corresponding official
  Apache RocketMQ Java and Go client implementations. Use their semantics and underlying approach as a reference,
  adapting it to this client's architecture and .NET runtime rather than copying implementation details wholesale.
  Preserve established protocol semantics unless a documented protocol-specific reason requires this client to differ.

## Architecture constraints

- Production contains exactly two independent client projects: `EventHorizon.RocketMQ.Grpc` for RocketMQ 5 Proxy/gRPC
  and `EventHorizon.RocketMQ.Remoting` for classic NameServer/Broker Remoting. Do not add a shared production project,
  transport-neutral root client, cross-protocol `ProjectReference`, common builder, or transport selector.
- Public APIs, options, models, results, exceptions, and DI registrations remain protocol-specific. Do not introduce
  transport-independent Producer, Consumer, Push, Pull, Simple, queue, result, status, or options abstractions.
- Matching foundational models are separate CLR types in each protocol project. Review the counterpart when changing
  one, but apply the change only when its semantics remain valid for both protocols.
- Keep reusable transport infrastructure under that protocol's `Protocol` folder. Feature scheduling, retries,
  transactions, headers, decoders, and lifecycle helpers stay beside their owning Producer or Consumer feature. Do not
  introduce generic `Common`, `Internal`, or `DependencyInjection` folders.
- Register gRPC through `AddRocketMQGrpc` and Remoting through `AddRocketMQRemoting`. Consumer engines are constructed
  at their protocol composition root, injected through the protocol-specific internal engine interface, and owned by
  the registered role; they are not global services.
- Namespaces mirror their owning project and source directory. Keep one top-level type per C# file, except for nested
  implementation details and test helpers.

## Transport constraints

- gRPC connects through a Proxy `Endpoint`; Remoting discovers Brokers through `NamesrvAddr`. gRPC Push and LitePush
  use client-initiated assignment queries and long polling, not protocol-level Broker push. Remoting Push also long
  polls but has classic compatibility and callback behavior, so do not assume the transports have identical features.
- Remoting uses the built-in `System.IO.Pipelines` transport. Do not reintroduce Bedrock Framework.
- Change checked-in `.proto` files for wire-contract changes; never edit generated files under `bin` or `obj`.

## Documentation

- Update documentation when behavior, configuration, public APIs, commands, architecture, setup, or user-facing
  functionality changes.
- Keep English and Simplified Chinese README or design-note pairs semantically synchronized. The root READMEs stay
  concise; protocol detail belongs in the protocol guides and design notes.
- Keep `src/EventHorizon.RocketMQ.Grpc/README*.md` and `src/EventHorizon.RocketMQ.Remoting/README*.md` as concise
  package user guides. They may cover installation, package selection, public API usage, configuration, deployment
  prerequisites, common errors, and links to runnable samples. Put architecture, wire commands, internal state
  machines, queue ownership, DI composition and lifetimes, telemetry topology, and test design only in the matching
  bilingual articles under `docs`, then link to those articles from the package guide when users need the detail.
- After changing a Simplified Chinese README, perform a dedicated editorial pass after semantic synchronization.
  Preserve code, commands, identifiers, links, technical facts, and boundary conditions while removing literal
  translation, repetitive templates, vague claims, and unnatural wording.
- Update the matching design note when an architectural, DI-lifecycle, transport, or testing decision changes. Update
  the relevant test-environment guide when a Compose environment changes.
- Do not change documentation for an internal refactor with no user-visible or architectural effect. State any
  documentation that could not be updated and why.

## C# and dependencies

- Target the frameworks declared by the project files and use only C# 12 language features. Keep nullable annotations,
  cancellation propagation, and `ConfigureAwait(false)` usage correct and consistent with nearby library code.
- Every C# file must use the Apache header from `.editorconfig`; keep configured analyzer diagnostics clean.
- Prefer concise modern C# when it clarifies the code. Use primary constructors for straightforward dependency or state
  initialization, but retain explicit constructors when validation, defensive copies, defaults, registrations, or
  resource cleanup make the lifecycle clearer.
- Write code comments and XML documentation in English. Document every public API type and member; do not suppress
  `CS1591` project-wide.
- Prefer constructor injection and standard Microsoft DI. Add an interface only for a real replacement or testing
  boundary; do not abstract data objects, options, framework types, or internal implementation details by default.
- Do not use generic `Worker` names for application types, services, variables, or background processes. Name each
  construct for its concrete responsibility, such as `MessageReceiveLoop` or `OffsetCommitLoop`.
- Keep each class focused on one cohesive responsibility. When orchestration, scheduling, registration lifetime,
  mutable state management, and protocol I/O start accumulating in one class, extract clearly owned internal
  collaborators instead of continuing to grow the class. Do not split code mechanically when the extracted type
  would have no independent responsibility or would only add indirection.
- Prefer base libraries and existing dependencies. Explain any new production dependency and compatibility impact.
  Do not add a repository `NuGet.config`, hard-code NuGet.org, or create an internal shared package.

## Testing and validation

- For substantial public-API, architectural-boundary, transport-behavior, or consumer-model changes, use this order:
  first write or update the relevant bilingual design note; next define the public API contract; then add the focused
  unit and integration tests; finally implement the production behavior. The API-contract phase may include only the
  minimum compile-only scaffolding needed for tests to express the contract; it must not contain the real behavior.
  Run the new behavioral tests before implementation and confirm that they fail for the intended missing behavior.
  Small bug fixes, internal refactors, documentation-only work, and mechanical configuration changes do not require a
  new design note or a separate API-contract phase unless they actually change one of those boundaries.
- Prefer test-driven development for behavior changes. Start with the smallest test that expresses the intended
  contract, run it, and confirm that it fails for the expected reason before changing production code. Then implement
  the smallest coherent change that makes the test pass, rerun the focused test, and finish with the affected complete
  test project and broader checks required by the change.
- Do not manufacture a failing test for documentation, mechanical configuration changes, or pure refactors whose
  behavior is already covered. For those cases, state why a meaningful red phase does not apply and run the relevant
  characterization, build, formatting, or verification checks instead.
- Put deterministic isolated tests in `tests/ut`; put Docker-backed behavior in the matching project under `tests/it`;
  keep reusable Testcontainers code in `tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure`; keep performance
  work under `tests/benchmarks`.
- Unit tests must not require an external RocketMQ installation or network access. The integration-test infrastructure
  is not a test assembly and must not reference a production protocol project.
- Add or update tests for behavior changes. Use xUnit v3 and normally strict Moq mocks. A stateful fake is appropriate
  only when a mock would obscure streaming, framing, or concurrency behavior.
- Name test methods `MemberOrBehavior_Scenario_ExpectedOutcome`, following the
  [Microsoft .NET unit-testing guidance](https://learn.microsoft.com/en-us/dotnet/core/testing/unit-testing-best-practices#naming-your-tests).
  Use PascalCase within each segment and underscores between the three segments. For an integration workflow without
  one method under test, name the workflow in the first segment. Avoid `Test` prefixes, `Should` filler, and
  sentence-style names.
- Integration coverage does not replace unit coverage for deterministic client-owned behavior. Even when an
  integration test covers the full workflow, add or retain focused unit tests for the underlying state transitions,
  allocation, scheduling, offset, retry, and failure invariants; use integration tests to complement them at real
  Broker, NameServer, Proxy, transport, persistence, and cross-process boundaries.
- Integration suites must cover both the normal workflow and relevant abnormal behavior that can be exercised
  deterministically at those real boundaries, including failure isolation, retries, blocked work, recovery, and
  persistence when the live environment provides a stable trigger. Low-level transport or persistence failures that
  cannot be injected reliably remain mandatory unit-test cases; do not replace them with flaky Docker fault injection.
- Matching foundational-model changes require both protocol unit-test projects and the compatibility project. Run the
  matching integration project when behavior crosses a live Broker, NameServer, Proxy, or transport boundary and
  Docker is available.
- After C# changes, run the narrowest relevant tests while iterating, then the affected complete test project. Run
  formatting before finishing.

Standard checks from the repository root:

```bash
dotnet format EventHorizon.RocketMQ.slnx
dotnet restore EventHorizon.RocketMQ.slnx
dotnet build EventHorizon.RocketMQ.slnx --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Grpc.Tests/EventHorizon.RocketMQ.Grpc.Tests.csproj --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Remoting.Tests/EventHorizon.RocketMQ.Remoting.Tests.csproj --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Compatibility.Tests/EventHorizon.RocketMQ.Compatibility.Tests.csproj --no-restore
```

For live transport changes, also run the relevant commands from the testing guide. Validate any changed Compose file
with `docker compose -f <environment>/compose.yaml config --quiet`. Clearly state a required command that could not run.
