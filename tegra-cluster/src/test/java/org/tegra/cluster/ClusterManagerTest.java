package org.tegra.cluster;

import org.junit.jupiter.api.Test;
import org.tegra.cluster.partition.HashPartitioning;
import org.tegra.cluster.partition.PartitionStrategy;
import org.tegra.store.partition.PartitionStore;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the ClusterManager.
 */
class ClusterManagerTest {

    @Test
    void nodeRegistrationAndLookup() {
        PartitionStore store0 = new PartitionStore();
        PartitionStore store1 = new PartitionStore();

        NodeDescriptor node0 = new NodeDescriptor("host0", 8080, 0, List.of(0, 1));
        NodeDescriptor node1 = new NodeDescriptor("host1", 8081, 1, List.of(2, 3));

        ClusterManager manager = new ClusterManager(
                List.of(node0, node1),
                new HashPartitioning(),
                4,
                Map.of(0, store0, 1, store0, 2, store1, 3, store1)
        );

        assertThat(manager.nodes()).containsExactly(node0, node1);
        assertThat(manager.numPartitions()).isEqualTo(4);
    }

    @Test
    void partitionAssignment() {
        NodeDescriptor node0 = new NodeDescriptor("host0", 8080, 0, List.of(0, 1));
        NodeDescriptor node1 = new NodeDescriptor("host1", 8081, 1, List.of(2, 3));

        PartitionStore store = new PartitionStore();

        ClusterManager manager = new ClusterManager(
                List.of(node0, node1),
                new HashPartitioning(),
                4,
                Map.of(0, store, 1, store, 2, store, 3, store)
        );

        assertThat(manager.nodeForPartition(0)).isEqualTo(node0);
        assertThat(manager.nodeForPartition(1)).isEqualTo(node0);
        assertThat(manager.nodeForPartition(2)).isEqualTo(node1);
        assertThat(manager.nodeForPartition(3)).isEqualTo(node1);
    }

    @Test
    void vertexToPartitionMapping() {
        PartitionStore store = new PartitionStore();
        NodeDescriptor node = new NodeDescriptor("host0", 8080, 0, List.of(0, 1, 2, 3));

        ClusterManager manager = new ClusterManager(
                List.of(node),
                new HashPartitioning(),
                4,
                Map.of(0, store, 1, store, 2, store, 3, store)
        );

        // Verify all vertex-to-partition mappings are valid
        for (long vid = 0; vid < 100; vid++) {
            int partition = manager.partitionForVertex(vid);
            assertThat(partition)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(4);
        }
    }

    @Test
    void localPartitions() {
        PartitionStore store0 = new PartitionStore();
        PartitionStore store1 = new PartitionStore();

        NodeDescriptor node = new NodeDescriptor("host0", 8080, 0, List.of(0, 1, 2));

        ClusterManager manager = new ClusterManager(
                List.of(node),
                new HashPartitioning(),
                3,
                Map.of(0, store0, 1, store0, 2, store1)
        );

        List<Integer> localParts = manager.localPartitions();
        assertThat(localParts).containsExactlyInAnyOrder(0, 1, 2);
    }

    @Test
    void storeForPartition() {
        PartitionStore store0 = new PartitionStore();
        PartitionStore store1 = new PartitionStore();

        NodeDescriptor node = new NodeDescriptor("host0", 8080, 0, List.of(0, 1));

        ClusterManager manager = new ClusterManager(
                List.of(node),
                new HashPartitioning(),
                2,
                Map.of(0, store0, 1, store1)
        );

        assertThat(manager.storeForPartition(0)).isSameAs(store0);
        assertThat(manager.storeForPartition(1)).isSameAs(store1);
    }

    @Test
    void storeForUnknownPartitionThrows() {
        PartitionStore store = new PartitionStore();
        NodeDescriptor node = new NodeDescriptor("host0", 8080, 0, List.of(0));

        ClusterManager manager = new ClusterManager(
                List.of(node),
                new HashPartitioning(),
                1,
                Map.of(0, store)
        );

        assertThatThrownBy(() -> manager.storeForPartition(99))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void startValidatesAllPartitionsAssigned() {
        PartitionStore store = new PartitionStore();
        // Node only has partition 0, but numPartitions is 2
        NodeDescriptor node = new NodeDescriptor("host0", 8080, 0, List.of(0));

        ClusterManager manager = new ClusterManager(
                List.of(node),
                new HashPartitioning(),
                2,
                Map.of(0, store)
        );

        assertThatThrownBy(manager::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Partition 1");
    }

    @Test
    void startSucceedsWhenAllPartitionsAssigned() {
        PartitionStore store = new PartitionStore();
        NodeDescriptor node = new NodeDescriptor("host0", 8080, 0, List.of(0, 1));

        ClusterManager manager = new ClusterManager(
                List.of(node),
                new HashPartitioning(),
                2,
                Map.of(0, store, 1, store)
        );

        // Should not throw
        manager.start();
        manager.stop();
    }
}
