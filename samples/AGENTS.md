# AGENTS.md

These instructions apply to every project under `samples/`.

## Sample design

- Give each sample one clear SDK workflow and make that workflow runnable. A reader should be able to identify the
  client registration, public role, important method calls, and completion or settlement behavior from the main code
  path.
- Show the protocol-specific public API directly. Do not introduce wrappers, factories, shared sample frameworks, or
  supporting machinery that readers must understand before they can understand the SDK.
- Keep supporting application code proportional to the workflow. Use a Generic Host, Web API, background service, or
  dependency injection only when it demonstrates normal integration with the SDK or is needed to run the example.
- Prefer explicit, deterministic example values at the point of use when configurability would add indirection without
  teaching anything. Make values configurable when users genuinely need to change them to run the sample in another
  environment.

## Ownership and structure

- Keep SDK concerns separate from behavior owned by the sample application. Do not duplicate one decision across
  both boundaries or let surrounding application plumbing dictate the SDK workflow.
- Keep resource names, filters, message construction, processing decisions, and failure behavior close to the public
  SDK call that uses them unless the same value must be shared by multiple real application components.
- Add an abstraction only when it removes meaningful duplication or demonstrates an integration boundary users should
  copy. Do not abstract a single call, data object, or fixed example value.
- Keep the gRPC and classic Remoting examples protocol-specific. Do not hide their different endpoints, capabilities,
  or delivery semantics behind a transport-neutral sample layer.

## API coverage

- Demonstrate the complete core workflow for the selected role. Keep acknowledgements, offset commits, settlement,
  cancellation, and relevant failure handling visible instead of implying success before the SDK operation completes.
- When a public API or materially different mode is added, check the sample set for a coverage gap. Add direct usage to
  an existing sample only when it fits that sample's primary workflow without making the main path harder to follow.
- Put workflows that require different Broker resources, server capabilities, or a cooperating peer in a dedicated
  sample or environment. Otherwise document the deliberate omission and link to the protocol guide; do not make the
  basic sample unreliable just to exercise every overload.
- Do not manufacture artificial work merely to call an API. The demonstrated operation should have a reason in the
  sample's workflow, and comments must explain protocol semantics or a non-obvious consequence rather than narrating
  obvious code.

## Documentation and validation

- Treat each README as a guide to the SDK mode being demonstrated, not as a narration of the demo program. Lead with
  what the mode is, when to choose or avoid it, its public registration and operation APIs, and its delivery,
  settlement, offset, concurrency, and failure semantics.
- Keep sample-specific details proportional. Include the shortest useful run command and the external prerequisites
  needed to make it work, but do not make fixed example values, logging behavior, helper types, or a copy of
  `appsettings.json` the main documentation.
- Document required resources and server capability limits where they affect the SDK mode. Do not present a
  server-dependent behavior as a universal SDK guarantee.
- Keep each sample's English and Simplified Chinese README semantically synchronized with its code and configuration.
- Keep local fixture names and prerequisites consistent with `test-environments/`.
- Build every affected sample. Run the matching local environment when behavior crosses a live Proxy, NameServer, or
  Broker boundary and the environment supports the feature. Build the solution when shared conventions or several
  projects change.
