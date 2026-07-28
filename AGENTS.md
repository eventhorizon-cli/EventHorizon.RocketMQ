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
- [gRPC consumer model](docs/en-US/grpc/consumer-model.md) and
  [classic Remoting transport and roles](docs/en-US/remoting/transport-and-client-roles.md): transport semantics.
- [Local and integration testing](docs/en-US/testing/local-and-integration-testing.md): test projects, Testcontainers,
  CI, coverage, and Compose boundaries.
- [Protocol guides](src/EventHorizon.RocketMQ.Grpc/README.md) and
  [classic Remoting guide](src/EventHorizon.RocketMQ.Remoting/README.md): public APIs, configuration, compatibility,
  and examples.
- [Samples index](samples/README.md) and [test-environment index](test-environments/README.md): runnable local
  setups and their prerequisites.

Read the relevant reference before changing an architectural boundary, transport behavior, DI lifecycle, test
topology, or manual environment.

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
- Prefer base libraries and existing dependencies. Explain any new production dependency and compatibility impact.
  Do not add a repository `NuGet.config`, hard-code NuGet.org, or create an internal shared package.

## Testing and validation

- Put deterministic isolated tests in `tests/ut`; put Docker-backed behavior in the matching project under `tests/it`;
  keep reusable Testcontainers code in `tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure`; keep performance
  work under `tests/benchmarks`.
- Unit tests must not require an external RocketMQ installation or network access. The integration-test infrastructure
  is not a test assembly and must not reference a production protocol project.
- Add or update tests for behavior changes. Use xUnit v3 and normally strict Moq mocks. A stateful fake is appropriate
  only when a mock would obscure streaming, framing, or concurrency behavior.
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
