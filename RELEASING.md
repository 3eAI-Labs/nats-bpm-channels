# Releasing to Maven Central

Publishing goes through the **Central Portal** (`central.sonatype.com`).

> The old OSSRH path (`issues.sonatype.org` JIRA tickets, `s01.oss.sonatype.org` staging,
> `nexus-staging-maven-plugin`) **was shut down on 30 June 2025**. Accounts created on the Central
> Portal have no access to it. Any instructions mentioning a JIRA ticket for a groupId are obsolete.

## One-Time Setup

### 1. Namespace

The namespace is **`com.3eai-labs`**, the reverse-DNS form of `3eai-labs.com`.

> `com.3eai` is **not** available: `3eai.com` has been registered to a third party since
> August 2022, and a namespace can only be verified by proving control of the matching domain.

1. Log in at https://central.sonatype.com, click your username → **View Namespaces**
2. **Add Namespace** → `com.3eai-labs`
3. Copy the **Verification Key**
4. In the `3eai-labs.com` DNS zone (Namecheap), add:

   | Type | Host | Value |
   |------|------|-------|
   | TXT  | `@`  | *(the Verification Key)* |

5. Back in the portal, click **Verify Namespace** — the check reads the TXT record on the exact
   apex domain `3eai-labs.com`. Subdomains are not consulted.

A verified namespace also covers every sub-namespace (`com.3eai-labs.*`), so future libraries need
no further verification.

### 2. User Token

The publish credentials are **not** your portal login. In the portal: username → **View Account** →
**Generate User Token**. This yields a username/password pair used as `CENTRAL_USERNAME` /
`CENTRAL_PASSWORD`.

### 3. GPG Key

Every artifact must carry a `.asc` signature from a key published to a public keyserver.

```bash
# Generate (RSA 4096). Use a real email you control.
gpg --full-generate-key

# Find the long key id
gpg --list-secret-keys --keyid-format=long

# Publish the PUBLIC key — Central verifies signatures against a keyserver
gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>

# Export the PRIVATE key for the GitHub secret, then delete the file
gpg --armor --export-secret-keys <KEY_ID> > private-key.asc
```

`scripts/gpg-setup.sh` automates the above and never echoes the passphrase.

### 4. GitHub Repository Secrets

**Settings → Secrets and variables → Actions**:

| Secret | Value |
|--------|-------|
| `CENTRAL_USERNAME` | User Token username (step 2) |
| `CENTRAL_PASSWORD` | User Token password (step 2) |
| `GPG_PRIVATE_KEY` | Full contents of `private-key.asc`, including the BEGIN/END lines |
| `GPG_PASSPHRASE` | The passphrase protecting that key |

Delete `private-key.asc` afterwards.

## What Gets Published

| Artifact | Published | Note |
|----------|-----------|------|
| `nats-channel-parent` | ✅ (pom only) | Required so child poms resolve their parent |
| `nats-core` | ✅ | |
| `flowable-nats-channel` | ✅ | |
| `camunda-nats-channel` | ✅ | |
| `cibseven-nats-channel` | ✅ | |
| `cadenzaflow-nats-channel` | ✅ | |
| `nats-history-projection` | ✅ | |
| `nats-bpm-bench` | ❌ | Benchmark harness; leaf module, nothing depends on it |

`nats-bpm-bench` is excluded via `<excludeArtifacts>` in the parent pom's
`central-publishing-maven-plugin` configuration. **`maven.deploy.skip` alone is not enough** — that
plugin does not honour the property (upstream request: mavenplugins/central-publishing-maven-plugin#22).

## Release Process

### 1. Dry run locally (do this first)

Builds sources + javadoc jars and proves nothing will be rejected. Javadoc errors are the most
common cause of a failed release — doclint fails the build on a broken `{@link}` or a bare `<` in
a doc comment.

```bash
export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64
mvn verify -P release -DskipTests -Dgpg.skip=true
```

To see *every* javadoc problem in one pass instead of module-by-module:

```bash
mvn verify -P release -DskipTests -Dgpg.skip=true \
    -Dmaven.javadoc.failOnError=false --fail-at-end 2>&1 | grep "error:"
```

### 2. Set the version

```bash
mvn versions:set -DnewVersion=0.7.0 -DgenerateBackupPoms=false
git commit -am "release: v0.7.0"
```

### 3. Tag and push

```bash
git tag v0.7.0
git push origin main --tags
```

This triggers `.github/workflows/release.yml`, which builds, signs, uploads, and waits for
validation.

### 4. Publish in the portal

`autoPublish` is **false**, so a successful workflow leaves the deployment **VALIDATED**, not
published. Go to https://central.sonatype.com/publishing/deployments and press **Publish**.

This gate is deliberate: **a published artifact can never be removed from Central.** Once you trust
the pipeline, set `<autoPublish>true</autoPublish>` in the parent pom's plugin configuration.

### 5. Verify

Artifacts appear at https://central.sonatype.com/artifact/com.3eai-labs/nats-core and reach
`repo1.maven.org` within roughly 30 minutes (search indexing can take a few hours longer).

## Troubleshooting

| Symptom | Cause |
|---------|-------|
| `401 Unauthorized` on upload | Using portal login instead of a User Token, or `server-id` ≠ `central` |
| `gpg: signing failed: Inappropriate ioctl for device` | Missing `--pinentry-mode loopback` (already configured in the parent pom) |
| `Namespace com.3eai-labs is not allowed` | TXT record missing/not yet propagated, or namespace not verified in the portal |
| Deployment stuck at `VALIDATED` | Expected — press **Publish** in the portal (see step 4) |
| `reference not found` / `unknown tag` during javadoc | Broken `{@link}`, or a bare `<...>` in a doc comment — wrap it in `{@code ...}` |
