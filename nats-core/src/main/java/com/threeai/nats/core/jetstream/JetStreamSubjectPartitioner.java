package com.threeai.nats.core.jetstream;

import io.nats.client.api.SubjectTransform;

/**
 * {@code SubjectTransform}/{@code Partition(N,token)} provisioning helper (ARCH-Q3 + LLD-Q2,
 * `docs/sentinel/step2/phase4/lld/history-offload/01_overview.md` "Phase3'ün devrettiği
 * doğrulamalar #2"). Builds the deterministic subject-mapped partitioning transform for the
 * {@code HISTORY} stream (`history.<engineId>.<class>.<processInstanceId>` publish-time subject →
 * {@code history.<engineId>.<class>.<processInstanceId>.part.<N>}) and the matching
 * {@code filter_subject} pattern a partition-bound durable consumer subscribes with.
 *
 * <p>Engine-neutral and reusable beyond the history subject shape — parameterized by base subject
 * and wildcard-token count so a future basamak (or a different 3-token subject family) can reuse
 * it without a new class (`02_package_structure.md` §2: "(genişler) JetStreamSubjectPartitioner").
 *
 * <p><b>CODER-NOTE:</b> {@code 01_overview.md} describes the consumer filter as literally
 * {@code history.>.part.<i>} — that is not a syntactically valid NATS subject (`>` may only be the
 * final token). This class implements the semantically-equivalent, syntactically-valid form
 * {@code history.*.*.*.part.<i>} (one {@code *} per wildcard token before the {@code .part.<i>}
 * suffix) which matches exactly the same set of post-transform subjects.
 */
public final class JetStreamSubjectPartitioner {

    private static final String PARTITION_TOKEN = ".part.";

    private JetStreamSubjectPartitioner() {
    }

    /**
     * @param baseSubject         e.g. {@code "history"} — the transform source is
     *                            {@code <baseSubject>.>}
     * @param wildcardCount       number of literal tokens after {@code baseSubject} in the
     *                            publish-time subject (3 for {@code <engineId>.<class>.<processInstanceId>})
     * @param partitionTokenIndex which wildcard token (1-based) the partition hash is computed
     *                            over (3 = {@code processInstanceId}, per ARCH-Q3/D-E)
     * @param partitionCount      N, number of partitions (default 8, LLD-Q2)
     */
    public static SubjectTransform buildPartitionTransform(String baseSubject, int wildcardCount,
            int partitionTokenIndex, int partitionCount) {
        requirePositive(wildcardCount, "wildcardCount");
        requirePositive(partitionCount, "partitionCount");
        if (partitionTokenIndex < 1 || partitionTokenIndex > wildcardCount) {
            throw new IllegalArgumentException(
                    "partitionTokenIndex must be within [1, wildcardCount] — got " + partitionTokenIndex);
        }
        String source = baseSubject + ".>";
        StringBuilder destination = new StringBuilder(baseSubject);
        for (int i = 1; i <= wildcardCount; i++) {
            destination.append('.').append("{{wildcard(").append(i).append(")}}");
        }
        destination.append(PARTITION_TOKEN)
                .append("{{Partition(").append(partitionCount).append(',').append(partitionTokenIndex).append(")}}");
        return new SubjectTransform(source, destination.toString());
    }

    /**
     * Builds the {@code filter_subject} a single partition's durable consumer binds to.
     *
     * @param partitionIndex 0-based partition index (must be {@code < partitionCount} used to
     *                       build the matching {@link #buildPartitionTransform})
     */
    public static String partitionFilterSubject(String baseSubject, int wildcardCount, int partitionIndex) {
        requirePositive(wildcardCount, "wildcardCount");
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must be >= 0 — got " + partitionIndex);
        }
        StringBuilder filter = new StringBuilder(baseSubject);
        for (int i = 0; i < wildcardCount; i++) {
            filter.append(".*");
        }
        filter.append(PARTITION_TOKEN).append(partitionIndex);
        return filter.toString();
    }

    /**
     * Resolves which replica owns which partition (`08_config.md` §4 — either an explicit
     * {@code partitionAssignment} list or {@code replicaOrdinal % partitionCount}).
     */
    public static int resolvePartitionIndex(int replicaOrdinal, int partitionCount) {
        requirePositive(partitionCount, "partitionCount");
        return Math.floorMod(replicaOrdinal, partitionCount);
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0 — got " + value);
        }
    }
}
