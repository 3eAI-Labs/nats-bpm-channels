package com.threeai.nats.core.jetstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.nats.client.api.SubjectTransform;
import org.junit.jupiter.api.Test;

class JetStreamSubjectPartitionerTest {

    @Test
    void buildPartitionTransform_historyDefault_matchesLldMechanism() {
        SubjectTransform transform = JetStreamSubjectPartitioner.buildPartitionTransform("history", 3, 3, 8);

        assertThat(transform.getSource()).isEqualTo("history.*.*.*");
        assertThat(transform.getDestination())
                .isEqualTo("history.{{wildcard(1)}}.{{wildcard(2)}}.{{wildcard(3)}}.part.{{Partition(8,3)}}");
    }

    @Test
    void partitionFilterSubject_matchesTransformDestinationShape() {
        String filter = JetStreamSubjectPartitioner.partitionFilterSubject("history", 3, 5);

        assertThat(filter).isEqualTo("history.*.*.*.part.5");
    }

    @Test
    void resolvePartitionIndex_modulo_wrapsCorrectly() {
        assertThat(JetStreamSubjectPartitioner.resolvePartitionIndex(0, 8)).isEqualTo(0);
        assertThat(JetStreamSubjectPartitioner.resolvePartitionIndex(7, 8)).isEqualTo(7);
        assertThat(JetStreamSubjectPartitioner.resolvePartitionIndex(8, 8)).isEqualTo(0);
        assertThat(JetStreamSubjectPartitioner.resolvePartitionIndex(15, 8)).isEqualTo(7);
    }

    @Test
    void buildPartitionTransform_rejectsInvalidPartitionTokenIndex() {
        assertThatThrownBy(() -> JetStreamSubjectPartitioner.buildPartitionTransform("history", 3, 0, 8))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JetStreamSubjectPartitioner.buildPartitionTransform("history", 3, 4, 8))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void buildPartitionTransform_rejectsNonPositiveCounts() {
        assertThatThrownBy(() -> JetStreamSubjectPartitioner.buildPartitionTransform("history", 0, 1, 8))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JetStreamSubjectPartitioner.buildPartitionTransform("history", 3, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void partitionFilterSubject_rejectsNegativeIndex() {
        assertThatThrownBy(() -> JetStreamSubjectPartitioner.partitionFilterSubject("history", 3, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
