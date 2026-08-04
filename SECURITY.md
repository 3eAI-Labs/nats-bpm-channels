# Security Policy

## Supported versions

This project is pre-1.0, so only the most recent release receives fixes. There are no long-term
support branches.

| Version | Supported |
|---------|-----------|
| 0.8.x   | ✅ |
| 0.7.x   | ❌ — superseded by 0.8.0 |
| < 0.7.0 | ❌ — never reached Maven Central |

## Reporting a vulnerability

**Do not open a public issue.**

Use GitHub's private vulnerability reporting:
[**Report a vulnerability**](https://github.com/3eAI-Labs/nats-bpm-channels/security/advisories/new).
The report stays private between you and the maintainers until a fix is published.

Useful things to include, roughly in order of how much they help:

- Which module and version
- What an attacker gains — the impact matters more than the mechanism
- A reproduction, ideally a failing test or a minimal Spring Boot application
- Engine and version, and NATS server version, if either is involved

This is a small project. You will get an acknowledgement, and an honest estimate rather than a
promised deadline. If a report turns out to be a plain bug rather than a vulnerability, you will be
told so and asked to move it to a public issue.

## Scope

In scope: anything in this repository that lets an attacker read or modify data they should not
reach — subject-namespace escapes, injection through configuration or message content, credentials
or personal data reaching logs, failures in the pseudonymization and erasure paths, or a delivery
guarantee that silently does not hold.

Out of scope: vulnerabilities in the BPM engines themselves (report those to Flowable, Camunda,
CIBSeven or CadenzaFlow), in the NATS server, or in a deployment's own configuration — for example
a NATS server exposed without TLS or authentication. This library will use the transport security
you configure; it cannot add any you have not.

## Verifying artifacts

Releases on Maven Central are GPG-signed and the public key is published to
`keyserver.ubuntu.com`. To verify a downloaded artifact:

```bash
gpg --keyserver keyserver.ubuntu.com --recv-keys <KEY_ID_FROM_THE_ASC_FILE>
gpg --verify nats-core-0.7.0.jar.asc nats-core-0.7.0.jar
```

An artifact claiming to be from `com.3eai-labs` that carries no valid signature did not come from
this project.
