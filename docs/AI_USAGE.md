# How we used AI tools, and how we handled security and privacy

The submission rules ask every team that used AI tools to say how, and how it dealt with security and privacy. This page is our answer. Beacon was built by three people, each working with AI coding agents. People made the product decisions and merged every change; agents wrote most of the code, the tests and the reviews.

## Tools

| Who | Part | AI tools |
|---|---|---|
| alexey | (a) ingest, matcher, aggregator, API, deploy, load generator | Claude Code, Codex, Cursor |
| erop | (c) UI, CI | Claude Code, Codex |
| ilyaarch | (b) delivery: routing, channels, retries, escalation, throttles, runbooks, approvals | Claude Code (lead session), OpenCode subagents (GLM 5.3, Muse Spark 1.3) |

## How we worked

- **One shared board for coordination.** Each person's agents joined an agentbridge board. They agreed interfaces there (the ingest → delivery outbox contract, the UI ↔ API contract), claimed parts of the repository so two agents never edited the same files, and reported which branch and commit their work was on. When something needed a human (a product choice, a merge, anything irreversible), the agent asked its own person.
- **Spec first.** `docs/SPEC.md` was written and reviewed in two rounds before any code (PR #1). Each milestone (M0–M7, SPEC §11) started with a plan that a person approved, then code.
- **Agents review agents.** The code PRs got a review by a different agent or model, and the larger ones got several rounds: security audits of the delivery PRs, a fresh-eyes "Sol" review of M4/M5, and an API review of the people/teams endpoints. Findings were fixed in the same PR, re-verified, and noted on the PR.
- **Tests and CI gate code merges.** GitHub Actions runs the Rust workspace tests against real Postgres and Mailpit, Clippy and the UI tests on every PR, and code changes merged only with green CI. The load and outage numbers in the README come from CI runs or teammates' machines, each with its run link or commit; none are estimated.

### Delivery (part b) in more detail

- One lead Claude Code session owned planning, shared interfaces, database migrations, binary wiring, integration, running the test suites, and all board communication.
- Implementation and tests went to OpenCode subagents, two or three at once, each with a written brief: files it may touch, gates it must pass (build, tests, Clippy, rustfmt), and "no git, no board".
- The lead reviewed every subagent diff before committing. Larger changes got an independent review from a teammate's agent.

### Ingest, engine, API and deploy (part a) in more detail

- A Claude Code session did most of the implementation and testing: ingest, the matcher and aggregator, exactly-once checkpoints, the REST/SSE API, the compose image and the load generator. It ran each milestone from a plan alexey approved, and it posted contract changes on the board before coding them.
- Reviews came from other models, so the author never graded its own work. Codex ran repeated "Sol" correctness rounds (GPT-6.2-Sol; four rounds on PR #8 alone). A Cursor agent did the security audits: NUL bytes, body timeouts, regex and signature bounds, localhost-only ports. After each milestone an independent tester agent ran the suite, an end-to-end run and a code review. Every finding was fixed or answered in the PR; [progress.md](../progress.md) lists them.
- Claims are measured, not estimated. The engine's 20k events/s figure, the exact counts under `kill -9`, and the Mac latency and outage runs come from the load generator, with workload parameters in [loadtest/README.md](../loadtest/README.md) and the README.

### Spec and UI (part c) in more detail

- **Spec first, reviewed by another model.** erop's Claude Code session wrote `docs/SPEC.md` (PR #1). A GPT model reviewed it in two read-only rounds (findings in `docs/reviews/`), and the fixes and board agreements went into v0.2 before any code.
- **Contract before backend.** The UI was built against `ui/contract/openapi.yaml` and a local mock server (`ui/mock/`), so it didn't wait for the API. An annotated SSE fixture (`contract/fixtures/stream-basic.sse`) defines the client state the feed must end in; `npm test` replays it through the UI's reducer and the backend can replay it too.
- **Two review passes per change.** Each UI change went through a correctness loop with a GPT model (findings fixed and re-checked until clean), then a separate Claude Opus pass for security and privacy (CSRF header on writes, no secrets or raw payloads rendered, redaction kept). The passes ran one after the other, never in parallel.
- **People chose the look.** The agent offered options (for example several logo concepts), erop picked one, and the agent checked the result in the running demo at desktop and narrow widths before merging.


## Security and privacy

**What the agents could see and do**
- **No real data, anywhere.** Every person, team and event in the repository is synthetic: email addresses are on `example.test`, and phone numbers are in `+44 7700 900xxx`, the UK range reserved for fiction. The sample events come from the brief. No production system, customer data or real contact detail ever reached an AI tool.
- **No secrets in the repository or in prompts.** The only credentials in the repo are labelled demo values for the local stack (`GATEWAY_TOKEN`, `REPLY_SECRET` in `compose.yaml`). Real keys, such as an SMTP password or `ANTHROPIC_API_KEY`, come from the environment only.
- **Least privilege for subagents.** Delivery's OpenCode subagents ran without access to the board (its command was blocked) and without git, so only a reviewed lead session could commit, push or talk to teammates.
- **Humans hold the irreversible steps.** Merges, deletions and anything outward-facing needed a person's OK or a standing rule that person set.

**AI inside the product**
- **LLM mapping suggestions are off by default** (`BEACON_LLM=off`; `docker compose up` doesn't turn it on). With `mock`, no network is used. With `claude`, the model never sees an event: it gets the payload's field paths, each with a type and a short redacted example. Its answer is a suggestion that an admin previews and saves through the normal path.
- **Auto-suggested proposals** (noisy-rule throttles) are computed from Beacon's own alert history with no model involved. They go through the same accept/reject review as a person's proposal.

**Security we built and reviewed with AI help**
- Secret-looking fields are redacted before they reach alerts, samples, notifications or the LLM. Logs carry no message text, SMS codes or approval challenges, and phone numbers and addresses are masked.
- Approval codes are stored hashed, single-use, expire after 15 minutes and allow 5 attempts, and the approval logic refuses a decision from the person who asked. SMS replies reach the API only HMAC-signed with a timestamp. **Demo caveat:** the device simulator and Mailpit have no login, so in the demo any session can read the SMS codes and the signed approval links and reply as any number; a real SMS provider and mail relay remove that (see "What is mocked" in `docs/DELIVERY.md`).
- Every port binds to localhost, writes need the session's CSRF token, and the fake SMS gateway refuses to listen beyond loopback without a token.
- What is mocked, and its limits, are listed in the README and in `docs/DELIVERY.md` ("What is mocked"). How to swap in a real provider is in `docs/PROVIDERS.md`.
