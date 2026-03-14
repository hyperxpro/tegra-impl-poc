package org.tegra.cluster.barrier;

import org.junit.jupiter.api.Test;
import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the BarrierCoordinator two-phase commit protocol.
 */
class BarrierCoordinatorTest {

    /**
     * A test partition node that records the sequence of prepare/commit calls.
     */
    static class RecordingPartitionNode implements PartitionNode {

        private final int id;
        private final List<String> events;
        private final AtomicInteger prepareCounter;
        private final AtomicInteger commitCounter;

        RecordingPartitionNode(int id, List<String> events,
                               AtomicInteger prepareCounter, AtomicInteger commitCounter) {
            this.id = id;
            this.events = events;
            this.prepareCounter = prepareCounter;
            this.commitCounter = commitCounter;
        }

        @Override
        public void prepareCommit(ByteArray versionId) {
            int prepareOrder = prepareCounter.incrementAndGet();
            events.add("prepare:" + id + ":" + versionId + ":" + prepareOrder);
        }

        @Override
        public void commit(ByteArray versionId) {
            int commitOrder = commitCounter.incrementAndGet();
            events.add("commit:" + id + ":" + versionId + ":" + commitOrder);
        }
    }

    @Test
    void allPartitionsCommitWithSameVersion() {
        ByteArray versionId = ByteArray.fromString("v1");
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicInteger prepareCounter = new AtomicInteger(0);
        AtomicInteger commitCounter = new AtomicInteger(0);

        List<PartitionNode> nodes = List.of(
                new RecordingPartitionNode(0, events, prepareCounter, commitCounter),
                new RecordingPartitionNode(1, events, prepareCounter, commitCounter),
                new RecordingPartitionNode(2, events, prepareCounter, commitCounter)
        );

        BarrierCoordinator coordinator = new BarrierCoordinator(nodes);
        ByteArray result = coordinator.coordinateCommit(versionId).join();

        assertThat(result).isEqualTo(versionId);

        // Verify all 3 partitions were prepared and committed
        long prepareCount = events.stream().filter(e -> e.startsWith("prepare:")).count();
        long commitCount = events.stream().filter(e -> e.startsWith("commit:")).count();
        assertThat(prepareCount).isEqualTo(3);
        assertThat(commitCount).isEqualTo(3);

        // Verify all events reference the same version
        for (String event : events) {
            assertThat(event).contains(versionId.toString());
        }
    }

    @Test
    void preparePhaseBeforeCommitPhaseOrdering() {
        ByteArray versionId = ByteArray.fromString("v2");
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicInteger prepareCounter = new AtomicInteger(0);
        AtomicInteger commitCounter = new AtomicInteger(0);

        int numPartitions = 4;
        List<PartitionNode> nodes = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            nodes.add(new RecordingPartitionNode(i, events, prepareCounter, commitCounter));
        }

        BarrierCoordinator coordinator = new BarrierCoordinator(nodes);
        coordinator.coordinateCommit(versionId).join();

        // Extract prepare and commit events
        List<String> prepareEvents = events.stream()
                .filter(e -> e.startsWith("prepare:")).toList();
        List<String> commitEvents = events.stream()
                .filter(e -> e.startsWith("commit:")).toList();

        assertThat(prepareEvents).hasSize(numPartitions);
        assertThat(commitEvents).hasSize(numPartitions);

        // Find the index of the last prepare event and the first commit event
        int lastPrepareIdx = -1;
        int firstCommitIdx = events.size();

        for (int i = 0; i < events.size(); i++) {
            if (events.get(i).startsWith("prepare:")) {
                lastPrepareIdx = i;
            }
            if (events.get(i).startsWith("commit:") && i < firstCommitIdx) {
                firstCommitIdx = i;
            }
        }

        // All prepare events must come before any commit event
        assertThat(lastPrepareIdx)
                .as("Last prepare event should come before first commit event")
                .isLessThan(firstCommitIdx);
    }

    @Test
    void singlePartitionCommit() {
        ByteArray versionId = ByteArray.fromString("v3");
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicInteger prepareCounter = new AtomicInteger(0);
        AtomicInteger commitCounter = new AtomicInteger(0);

        PartitionNode singleNode = new RecordingPartitionNode(0, events, prepareCounter, commitCounter);
        BarrierCoordinator coordinator = new BarrierCoordinator(List.of(singleNode));

        ByteArray result = coordinator.coordinateCommit(versionId).join();

        assertThat(result).isEqualTo(versionId);
        assertThat(events).hasSize(2);
        assertThat(events.get(0)).startsWith("prepare:");
        assertThat(events.get(1)).startsWith("commit:");
    }
}
