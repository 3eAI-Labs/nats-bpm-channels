package com.threeai.nats.core.history;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Pure, no I/O — deterministic keyed-hash (BA-Q5,
 * `docs/sentinel/step2/phase4/lld/history-offload/03_classes/6_governance.md` §3). Called IN-TX by
 * {@code CompactHistoryOutboxWriter} (engine side, `camunda-nats-channel`/`cadenzaflow-nats-channel`)
 * AND by {@code PseudonymizationVaultClient} (projection side, `nats-history-projection`, for
 * map-write) — lives in {@code nats-core} so both call sites share the exact same
 * algorithm/tenant-key version (`02_package_structure.md` §2 note).
 *
 * <p><b>Tersinmezlik modeli (09_security/2_pii_protection.md §4):</b> the token itself is NOT
 * irreversible by construction (same {@code tenantKeyId}+{@code realValue} always produces the
 * same token) — irreversibility is achieved downstream by deleting the {@code pseudonym_map} row
 * that maps the token back to the real value (ADR-0016). This class only computes the token.
 *
 * <p><b>CODER-NOTE (key-material resolution):</b> the LLD signature is {@code generate(realValue,
 * tenantKeyId, tenantKeyVersion)} and the class is required to be "pure, no I/O" (03_classes/6
 * §3 Javadoc) — yet {@code 08_config.md} §1 describes {@code tenantKeyId} as "OpenBao/deploy-secret
 * referansı" (a REFERENCE, not the secret bytes themselves). Resolving an OpenBao reference to
 * actual key material is I/O by definition, so it cannot happen inside this pure function. This
 * implementation therefore treats whatever string value it is handed as the HMAC key material
 * VERBATIM — the config layer (Spring property binding backed by an OpenBao-injected environment
 * variable, per DEVELOPER_GUIDELINE.md §7 "Secrets Management") is responsible for resolving the
 * reference to real secret bytes BEFORE {@code HistoryClassificationProperties.tenantKeyId} is
 * populated at bootstrap. See CODER-QUESTIONS in the phase-5 return report.
 */
public final class PseudonymTokenGenerator {

    private static final String HMAC_ALGORITHM = "HmacSHA256";

    /**
     * @param realValue        the real value to pseudonymize (e.g. a user id) — never returned,
     *                         never logged by this method
     * @param tenantKeyId      HMAC key material for this tenant (see class Javadoc CODER-NOTE)
     * @param tenantKeyVersion rotation marker, folded into the keyed-hash so that rotating the
     *                         key produces a different token for the same real value (persisted
     *                         alongside the token in {@code pseudonym_map.tenant_key_version})
     * @return deterministic, lowercase hex-encoded HMAC-SHA256 digest (64 chars — fits
     *         {@code pseudonym_token VARCHAR(128)})
     */
    public String generate(String realValue, String tenantKeyId, int tenantKeyVersion) {
        if (realValue == null || realValue.isEmpty()) {
            throw new IllegalArgumentException("realValue must not be null/empty");
        }
        if (tenantKeyId == null || tenantKeyId.isEmpty()) {
            throw new IllegalArgumentException("tenantKeyId must not be null/empty");
        }
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            String keyMaterial = tenantKeyId + ":" + tenantKeyVersion;
            mac.init(new SecretKeySpec(keyMaterial.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
            byte[] digest = mac.doFinal(realValue.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            // HmacSHA256 is a JDK-mandatory algorithm and the key is never empty here — this is
            // an environment defect (broken JCE provider), not a business-flow branch.
            throw new IllegalStateException("Pseudonym token generation failed — HMAC provider unavailable", e);
        }
    }
}
