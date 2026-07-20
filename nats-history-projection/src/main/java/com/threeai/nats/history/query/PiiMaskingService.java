package com.threeai.nats.history.query;

/**
 * Masks RESTRICTED/PII fields (variable value, operator identity, free text) per DP-15 when
 * {@code ctx.piiViewPermitted()==false} (`09_security/2_pii_protection.md` §2). Never masks
 * INTERNAL/PUBLIC fields (activityId, historyEventId). Masking is applied AFTER the DB read,
 * BEFORE serialization — masked values never appear in {@code HistoryQueryApi}'s own logs (DP-1).
 */
public class PiiMaskingService {

    static final String MASK_PLACEHOLDER = "***";

    @SuppressWarnings("unchecked")
    public <T> T mask(T responseDto, QueryContext ctx) {
        if (ctx.piiViewPermitted() || responseDto == null) {
            return responseDto;
        }
        if (responseDto instanceof ProcessInstanceSummary dto) {
            return (T) dto.masked();
        }
        if (responseDto instanceof ActivityHistoryEntry dto) {
            return (T) dto.masked();
        }
        if (responseDto instanceof TaskHistoryEntry dto) {
            return (T) dto.masked();
        }
        if (responseDto instanceof VariableHistoryEntry dto) {
            return (T) dto.maskedCopy();
        }
        return responseDto; // unrecognized type -- pass through unmodified (no known PII fields)
    }
}
