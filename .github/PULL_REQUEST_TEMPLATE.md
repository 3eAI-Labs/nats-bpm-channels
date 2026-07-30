<!--
Development happens in a private repository and this one is published as a snapshot per release,
so your commit will not appear verbatim in this repository's history. The change itself is applied
upstream and credited in CHANGELOG.md. See CONTRIBUTING.md.
-->

## What this changes

<!-- And why. Link the issue it addresses. -->

## Mirror parity

`camunda-nats-channel`, `cibseven-nats-channel` and `cadenzaflow-nats-channel` are byte-mirrors of
each other. A change to one must be applied to all three, comments included.

- [ ] Not applicable — this does not touch a Camunda-lineage adapter
- [ ] Applied identically to all three

## Checks

- [ ] `mvn install` passes on Java 21
- [ ] Tests added or updated for behaviour changes, and they fail without the change
- [ ] `CHANGELOG.md` updated under `## [Unreleased]` if this is user-visible
- [ ] Comments are in English and explain something the code cannot say itself
