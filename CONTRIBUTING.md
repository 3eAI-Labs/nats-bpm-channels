# Contributing

Thanks for considering a contribution.

## How this repository is published

Development happens in a private repository; this one is published as a full-tree snapshot per
release. That is why the history here is short and its commits do not correspond one-to-one with
the work that produced them.

It does not change how you contribute. Open issues and pull requests here as normal — they are read
and answered here. An accepted change is applied upstream and appears in the next published
snapshot, credited in `CHANGELOG.md`. The one consequence worth knowing in advance: your commit will
not appear verbatim in this repository's history, so if attribution matters to you, say so in the
pull request and it will be recorded in the changelog entry.

## Building

```bash
git clone https://github.com/3eAI-Labs/nats-bpm-channels.git
cd nats-bpm-channels
mvn install
```

**Java 21 is required** — the build fails on newer JDKs. If your default JDK is different:

```bash
export JAVA_HOME=/path/to/jdk-21
```

Integration tests use [Testcontainers](https://testcontainers.com) and need a running Docker
daemon. Without one, `mvn install -DskipTests` builds the artifacts; a contribution that changes
behaviour still needs the tests run somewhere before it can be merged.

The reliability suite (fault injection, leader failover, split-brain and DLQ scenarios) is tagged
and excluded from the default run because it is slow:

```bash
mvn verify -Dgroups=reliability
```

## The three Camunda-lineage adapters are byte-mirrors

`camunda-nats-channel`, `cibseven-nats-channel` and `cadenzaflow-nats-channel` are the same code
targeting three engines. Normalising the package and class names
(`camunda` ↔ `cibseven` ↔ `cadenzaflow`, `org.camunda.*` ↔ `org.cibseven.*` ↔ `org.cadenzaflow.*`)
makes the three trees identical apart from a deliberate difference in each `pom.xml`
`<description>` and a few engine-specific test constants.

**A change to one must be applied to all three**, comments included. A pull request that touches
only one is incomplete and will be sent back. Keeping the mirror exact is what lets a fix land on
every Camunda-lineage engine at once.

Engine-neutral logic belongs in `nats-core` so it lives once rather than three times.
`flowable-nats-channel` is not a mirror — Flowable's Event Registry is a different integration
seam — so it is changed on its own terms.

## Pull requests

Open an issue first for anything beyond a small fix, so the approach can be agreed before you spend
time on it.

- Keep the change focused; unrelated cleanups make review harder
- Add or update tests for behaviour changes
- Match the style of the code you are editing rather than introducing a new one
- Write comments in English, and only where they explain something the code cannot say itself
- Update `CHANGELOG.md` under `## [Unreleased]` for anything user-visible
- Make sure `mvn install` passes before opening the pull request
- Accept the [contributor licence agreement](CLA.md) when the bot asks — once, on your first
  pull request

## Reporting bugs

Use the issue templates. The single most useful thing you can include is the engine and version, the
adapter version, and the NATS server version — most reports turn on one of those three.

Security vulnerabilities do **not** belong in a public issue. See [SECURITY.md](SECURITY.md).

## Licence and contributor agreement

This project is released under the [Apache License 2.0](LICENSE), and every contribution is
published under that licence.

Before a contribution can be merged you also have to accept the
[3eAI Labs Contributor Licence Agreement](CLA.md). You keep the copyright in what you write; the
agreement grants 3eAI Labs the right to distribute your contribution, including under licence terms
other than Apache 2.0. Read section 2 before accepting.

Accepting takes one pull request comment. A bot asks for it on your first pull request:

```
I have read the CLA Document and I hereby sign the CLA
```

You do this once. Without it the pull request status check stays red and the contribution cannot be
merged.

If you are contributing in the course of your employment, say so in the pull request — see section
4.2 of the agreement.
