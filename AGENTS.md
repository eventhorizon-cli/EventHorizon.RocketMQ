# AGENTS.md

You are an AI coding assistant for this repository.

## Scope and precedence

- These instructions apply to the entire repository unless a more specific `AGENTS.md` exists in a
  subdirectory.
- Always follow `.editorconfig`. Treat it as the highest-priority code-style rule.
- Follow nearby files for naming, structure, formatting, and local conventions.
- Keep changes focused, simple, maintainable, and production-ready.
- Do not modify unrelated user changes in the working tree.

## Repository layout

- `src/EventHorizon.RocketMQ.Grpc` owns the complete RocketMQ 5 gRPC client boundary. Its code is organized as
  `{Protocol,Consumer,Producer,Exceptions}`:
  - `Protocol` contains reusable endpoint and RPC clients, metadata, status handling, route services,
    telemetry sessions, body codecs, and protobuf definitions.
  - `Consumer` owns `ConsumeResult`, `ConsumerOptions`, `FilterExpression`, and `FilterExpressionType` as well as
    the receive engine and gRPC consumer features under `{Lite,Push,Simple}`. `GrpcMessageView` is a protocol-owned
    consumer model. Receive orchestration used by Push and Simple consumers remains directly under `Consumer` as
    the internal `IGrpcReceiveConsumerEngine` and `GrpcReceiveConsumerEngine`.
  - `Producer` owns `Message`, `IGrpcProducer`, `GrpcProducerOptions`, `GrpcSendReceipt`, the implementation, and
    `Transactions`.
  - `Exceptions` owns `RocketMQClientException` and `GrpcServiceException`.
  - `GrpcClientOptions` owns Proxy endpoints, credentials, namespace, TLS, and gRPC timing. gRPC client identifiers
    are generated internally per role; do not add classic Remoting identity options to it. It does not derive from a
    repository-defined client-options base class.
- `src/EventHorizon.RocketMQ.Remoting` owns the complete classic Remoting client boundary. Its code is organized as
  `{Protocol,Consumer,Producer,Exceptions}`:
  - `Protocol` contains reusable socket clients, connections, ACL, frame and JSON serialization,
    reusable wire types, and NameServer route services.
  - `Consumer` owns `ConsumeResult`, `ConsumerOptions`, `FilterExpression`, `FilterExpressionType`, and the
    Remoting-only `QueryOffsetPolicy`, as well as classic consumer features under `{Pull,Push}`. It also owns
    `RemotingMessageView`, `RemotingPullResult`, and `RemotingPullStatus`. Pull and Push orchestration uses the
    internal `IRemotingConsumerEngine` and `RemotingConsumerEngine` directly under `Consumer`.
  - `Producer` owns `Message`, `IRemotingProducer`, `RemotingProducerOptions`, `RemotingSendResult`,
    `RemotingSendStatus`, `RemotingMessageQueue`, the implementation, and request headers.
  - `Exceptions` owns `RocketMQClientException` and `RemotingCommandException`.
  - `RemotingClientOptions` owns NameServer discovery, classic client identity, credentials, namespace, TLS,
    transport limits, Remoting timing, and role-specific cloning. It does not derive from a repository-defined
    client-options base class.
- The production graph contains only the two protocol Projects, with no ProjectReference between them. Do not
  introduce a third production Project, a transport-neutral root client, or a production dependency from one
  protocol to the other.
- Namespaces must reflect project ownership and the relative source directory:
  - gRPC uses `EventHorizon.RocketMQ.Grpc` at the project root and appends directories such as `Consumer.Lite`,
    `Consumer.Push`, `Consumer.Simple`, `Producer.Transactions`, or `Protocol.Telemetry`.
  - Remoting uses `EventHorizon.RocketMQ.Remoting` at the project root and appends directories such as
    `Consumer.Pull`, `Consumer.Push`, `Producer`, `Exceptions`, or `Protocol.Route`.
  - Unit-test and benchmark namespaces mirror their project name and relative directory. Integration-test
    files at the project root use the integration-test project namespace.
  - Assembly attribute files and top-level `Program.cs` files do not require a namespace.
- `Message`, `ConsumeResult`, `ConsumerOptions`, `FilterExpression`, `FilterExpressionType`, and
  `RocketMQClientException` are physically declared in both protocol Projects under the matching protocol
  namespace. They are distinct CLR types and are not interchangeable. `QueryOffsetPolicy` exists only in Remoting.
- When changing one of the matching foundational models, review the counterpart in the other protocol Project.
  Apply the same change only when the semantics remain valid for both protocols. Keep the files physically separate;
  do not replace them with linked source, generated source, or a reintroduced Shared Project or Shared Package.
- Keep each internal hosted lifecycle helper beside the Producer or Consumer role that it starts and stops.
  Each protocol project owns its client options, builder, service-collection entry point, and role-registration
  metadata at the project root. Do not introduce a generic `Internal` or `DependencyInjection` folder.
- Construct each internal Consumer Engine in the matching protocol composition root and inject it through its
  protocol-specific internal interface. Each registered Consumer role owns one Engine instance and its lifecycle;
  do not register Consumer Engines as protocol-wide global services or instantiate them inside Consumer
  implementations.
- Register gRPC client registrations with `AddRocketMQGrpc` and classic Remoting client registrations with
  `AddRocketMQRemoting`. Do not introduce a transport selector, a common builder, or a common root
  registration method.
- Place code under a protocol's `Protocol` folder only when it is communication infrastructure reused across
  features. Consumer scheduling, Producer sending, retries, transactions, and feature-specific headers or
  decoders stay with the owning Consumer or Producer feature. Do not introduce generic `Common` folders.
- Consumer service APIs are protocol-specific. gRPC uses `GrpcMessageView`, `IGrpcPushConsumer`,
  `GrpcPushConsumerOptions`, and `AddGrpcPushConsumer`, plus `IGrpcLitePushConsumer`,
  `GrpcLitePushConsumerOptions`, and `AddGrpcLitePushConsumer`. Classic Remoting uses `RemotingMessageView`,
  `RemotingPullResult`, `RemotingPullStatus`,
  `IRemotingPullConsumer`, `RemotingPullConsumerOptions`, `RemotingPullMessageQueue`, and
  `AddRemotingPullConsumer`, plus `IRemotingPushConsumer`, `RemotingPushConsumerOptions`, and
  `AddRemotingPushConsumer`. gRPC SimpleConsumer uses `IGrpcSimpleConsumer`, `GrpcSimpleConsumerOptions`, and
  `AddGrpcSimpleConsumer`. Do not introduce transport-independent Simple/Pull/Push consumer interfaces,
  message views, results, statuses, options, queue types, or registration methods.
- Producer service APIs are protocol-specific. gRPC uses `IGrpcProducer`, `GrpcProducerOptions`,
  `GrpcSendReceipt`, and `AddGrpcProducer`; classic Remoting uses `IRemotingProducer`,
  `RemotingProducerOptions`, `RemotingSendResult`, `RemotingSendStatus`, `RemotingMessageQueue`, and
  `AddRemotingProducer`. Do not introduce a transport-independent Producer interface, result, status, queue,
  options type, or registration method.
- `tests/ut/EventHorizon.RocketMQ.Grpc.Tests` and `tests/ut/EventHorizon.RocketMQ.Remoting.Tests` contain isolated xUnit
  unit tests for their matching production Project.
- `tests/ut/EventHorizon.RocketMQ.Compatibility.Tests` references both protocol Projects. It verifies both public APIs,
  distinct protocol namespaces, and the ability to consume both protocol assemblies together.
- `tests/it/EventHorizon.RocketMQ.Grpc.IntegrationTests` and `tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests` contain
  protocol-isolated Docker-backed integration tests.
- `tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure` contains reusable Testcontainers infrastructure and is
  not a test assembly. It must not reference a production protocol Project.
- `tests/benchmarks/EventHorizon.RocketMQ.Benchmarks` contains BenchmarkDotNet benchmarks.
- `test-environments` groups Docker Compose environments for manual local testing. Each environment includes a local
  RocketMQ Dashboard at `http://localhost:8082`:
  - `rocketmq` is the general single-Broker stack for standard Remoting and gRPC exploration.
  - `rocketmq-litepush` is the self-initializing gRPC LitePush sample stack; its Compose startup creates the
    repository's LITE parent Topic and Consumer Group.
  - `rocketmq-multi-broker` contains three independent master Brokers for route, partitioning, Remoting, and gRPC
    testing. It is not a high-availability or replication topology.
  Integration tests use their own Testcontainers fixture and do not require any manual environment to be started first.
- `README.md` and `README.zh-CN.md` are concise English and Simplified Chinese repository overviews that
  link to the protocol guides. Detailed gRPC documentation lives in `src/EventHorizon.RocketMQ.Grpc/README.md`
  and `README.zh-CN.md`; detailed classic Remoting documentation lives in
  `src/EventHorizon.RocketMQ.Remoting/README.md` and `README.zh-CN.md`.
- [`docs/README.md`](docs/README.md) is the language selector for the bilingual design notes. `docs/en-US` and
  `docs/zh-CN` use matching paths for architecture, gRPC, Remoting, and testing articles.

## Documentation rules

When modifying code, always consider whether related documentation must be updated.

- Update documentation together with changes to behavior, configuration, public APIs, commands,
  architecture, setup, or user-facing functionality.
- Keep each English and Simplified Chinese README pair semantically synchronized. Keep the root READMEs
  limited to the project overview, supported-feature summary, package selection, and links; place protocol
  configuration, API examples, compatibility notes, and operational details in the matching protocol guide.
- Update the matching `docs/en-US` and `docs/zh-CN` design article when an architectural decision, protocol
  boundary, dependency-injection lifecycle, transport model, or testing strategy changes. Keep the two language
  trees structurally and semantically synchronized.
- Update the matching `test-environments/<environment>/README.md` and `README.zh-CN.md` when a manual environment or
  its commands change.
- Keep examples consistent with the implemented API and supported server versions.
- Do not update documentation unnecessarily for internal refactors that do not affect behavior or public
  contracts.
- If required documentation cannot be updated in the current task, state what is affected and why.

## C# rules

These rules apply to C# files under both `src` and `tests`.

### File header

- Every C# file must use the exact Apache license header defined by `file_header_template` in
  `.editorconfig`.
- Do not shorten, reword, or replace that header with an older project header.
- Keep `dotnet_diagnostic.IDE0073` clean; new and modified C# files must not report a missing or mismatched
  header.

### General guidelines

- Use the .NET SDK pinned by `global.json` (currently 10.0.100). Do not change or bypass the pinned SDK as an
  incidental part of another task.
- Target .NET 8 and .NET 10, and use only language features supported by C# 12.
- Prefer C# and .NET best practices when they do not conflict with `.editorconfig` or nearby code.
- Keep nullable-reference-type annotations correct and do not suppress warnings without a concrete reason.
- Propagate `CancellationToken` through asynchronous operations where the API provides one.
- Use `ConfigureAwait(false)` in library implementation code when consistent with the surrounding code.
- Keep code readable and avoid unnecessary abstractions or complexity.

### Modern C# syntax

Use concise modern C# syntax when it is clearer and consistent with the repository, including:

- property and field initializers;
- getter-only properties and `init` accessors;
- expression-bodied members;
- `nameof`, string interpolation, and null operators;
- pattern matching, inline `out var`, tuples, and deconstruction;
- target-typed `new()` and collection expressions;
- `using` declarations and file-scoped namespaces;
- records and primary constructors.

Do not use newer syntax when it is unsupported, conflicts with `.editorconfig` or nearby style, or makes the
code harder to understand. Prefer initializers over constructor boilerplate for simple defaults and primary
constructors for straightforward dependency or state initialization.

### Types and comments

- Put exactly one top-level type in each C# file and align the file name with that type. This applies to
  classes, interfaces, records, structs, enums, and delegates.
- Nested implementation-detail types may remain with their owning type. Tests may also contain nested helper
  types, but additional top-level test types require separate files.
- Write all code comments, XML documentation, and TODO/FIXME comments in English.
- Document every publicly visible API type and member with standard XML documentation, including constructors,
  methods, properties, enum members, and exception types. Keep production projects free of `CS1591` warnings and
  do not suppress `CS1591` at the project level.
- Prefer self-explanatory code; add comments only when they explain a non-obvious constraint or decision.

### Dependency injection

- Prefer constructor injection and standard Microsoft dependency injection patterns.
- Depend on focused abstractions when doing so provides a meaningful boundary or improves testability.
- Register services through their public abstraction when consumers should not depend on the implementation.
- Keep composition in the protocol-specific service-collection extensions, builders, or another application
  boundary.
- Do not use service locators, hidden dependencies, or instantiate infrastructure dependencies inside business
  logic.
- Do not create interfaces for data objects, options, framework types, or implementation details that do not
  need an abstraction.

## Transport rules

- Keep the RocketMQ 5 gRPC path and classic Remoting path separated through their protocol-specific public
  interfaces, options, registrations, and implementations.
- gRPC connects to a RocketMQ Proxy through `Endpoint`; classic remoting discovers brokers through a
  NameServer configured with `NamesrvAddr`.
- Do not describe gRPC as offering protocol-level broker push. `IGrpcPushConsumer` is implemented through
  client-initiated assignment queries and repeated `ReceiveMessage` long polling followed by automatic
  dispatch and acknowledgement.
- `IRemotingPushConsumer` is also pull/long-poll based internally, but it additionally carries classic
  Remoting compatibility and Broker callback behavior. Do not assume the two transports have identical
  capabilities.
- Preserve server-version compatibility notes when changing protobuf APIs or consumer behavior.
- Classic remoting uses the repository's built-in `System.IO.Pipelines` transport. Do not reintroduce the
  Bedrock Framework dependency.
- Edit the checked-in `.proto` definitions when the wire contract changes. Never edit generated files under
  `bin` or `obj`.

## Dependency rules

- Prefer the .NET base class libraries and existing dependencies before adding a package.
- Explain the need and compatibility impact of any new production dependency.
- Do not introduce an internal common Package or make either protocol Package depend on another repository client
  Package. The gRPC and Remoting Packages must remain independently publishable.
- The repository intentionally does not provide a `NuGet.config`; restore uses package sources configured by
  the current environment.
- Do not add a repository-level `NuGet.config` or hard-code NuGet.org unless the task explicitly requires a
  package-source policy change.

## Testing rules

- Add or update tests for behavior changes and regressions.
- Use xUnit v3 conventions already present in the repository.
- Prefer Moq, normally with `MockBehavior.Strict`, for replaceable collaborators and interaction verification.
- Replace hand-written stubs with Moq when Moq can express the behavior clearly.
- Keep a purpose-built fake or stub only when it models stateful streaming, protocol framing, concurrency, or
  another behavior that would be substantially less clear with Moq.
- Unit tests must be deterministic and must not require an external RocketMQ installation or network access.
- Changes to matching foundational models require both protocol unit-test Projects and
  `tests/ut/EventHorizon.RocketMQ.Compatibility.Tests` to pass.
- Put Docker-backed behavior in the matching protocol integration-test project and reuse
  `tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure` for Testcontainers infrastructure.
- Add or update benchmarks only when the task affects a performance-sensitive path or explicitly requires a
  benchmark.

## Validation

After modifying C# code, run formatting and the relevant tests. From the repository root, the standard checks
are:

```bash
dotnet format EventHorizon.RocketMQ.slnx
dotnet restore EventHorizon.RocketMQ.slnx
dotnet build EventHorizon.RocketMQ.slnx --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Grpc.Tests/EventHorizon.RocketMQ.Grpc.Tests.csproj --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Remoting.Tests/EventHorizon.RocketMQ.Remoting.Tests.csproj --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Compatibility.Tests/EventHorizon.RocketMQ.Compatibility.Tests.csproj --no-restore
```

Rules:

- `dotnet format` is required after C# changes. Fix all formatting and file-header issues before finishing.
- Run the narrowest relevant tests while iterating, then run the complete unit-test project before finishing.
- Run the matching protocol integration-test project when behavior involving a live Broker, NameServer, Proxy,
  or transport interoperability changes and Docker is available:

```bash
dotnet test tests/it/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj --no-restore
dotnet test tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests/EventHorizon.RocketMQ.Remoting.IntegrationTests.csproj --no-restore
```

- Validate the affected test environment with its Compose file. For example:

```bash
docker compose -f test-environments/rocketmq/compose.yaml config --quiet
docker compose -f test-environments/rocketmq-litepush/compose.yaml config --quiet
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml config --quiet
```

- If a required command cannot be executed, clearly state which command was not run and why.
