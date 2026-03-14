package org.tegra.cluster.ingestion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.tegra.cluster.ClusterManager;
import org.tegra.cluster.NodeDescriptor;
import org.tegra.cluster.barrier.BarrierCoordinator;
import org.tegra.cluster.barrier.PartitionNode;
import org.tegra.cluster.partition.HashPartitioning;
import org.tegra.cluster.partition.PartitionStrategy;
import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.PropertyValue;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.log.GraphMutation;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the UpdateIngestionRouter.
 */
class UpdateIngestionRouterTest {

    private PartitionStrategy strategy;
    private PartitionStore store0;
    private PartitionStore store1;
    private ClusterManager cluster;
    private ByteArray initialVersion;

    @BeforeEach
    void setUp() {
        strategy = new HashPartitioning();
        store0 = new PartitionStore();
        store1 = new PartitionStore();

        initialVersion = ByteArray.fromString("v0");
        store0.createInitialVersion(initialVersion);
        store1.createInitialVersion(initialVersion);

        NodeDescriptor node = new NodeDescriptor("localhost", 8080, 0, List.of(0, 1));
        cluster = new ClusterManager(
                List.of(node),
                strategy,
                2,
                Map.of(0, store0, 1, store1)
        );
        cluster.start();
    }

    @Test
    void routesMutationsToCorrectPartitions() {
        // Create mutations for specific vertices
        long vid1 = 1L;
        long vid2 = 2L;
        int partition1 = strategy.partitionForVertex(vid1, 2);
        int partition2 = strategy.partitionForVertex(vid2, 2);

        List<GraphMutation> mutations = List.of(
                new GraphMutation.AddVertex(new VertexData(vid1, Map.of())),
                new GraphMutation.AddVertex(new VertexData(vid2, Map.of()))
        );

        // Create a no-op barrier coordinator
        BarrierCoordinator barrier = new BarrierCoordinator(List.of(
                new NoOpPartitionNode(), new NoOpPartitionNode()
        ));

        UpdateIngestionRouter router = new UpdateIngestionRouter(cluster, barrier);
        router.ingest(mutations, initialVersion);

        ByteArray newVersion = ByteArray.fromString("v1");
        router.commitAll(newVersion);

        // Verify vertices are in their correct partitions
        PartitionStore storeForVid1 = cluster.storeForPartition(partition1);
        PartitionStore storeForVid2 = cluster.storeForPartition(partition2);

        GraphView view1 = storeForVid1.retrieve(newVersion);
        GraphView view2 = storeForVid2.retrieve(newVersion);

        assertThat(view1.vertex(vid1)).isNotNull();
        assertThat(view2.vertex(vid2)).isNotNull();
    }

    @Test
    void routesEdgeMutationsCorrectly() {
        long srcId = 10L;
        long dstId = 20L;
        EdgeKey ek = new EdgeKey(srcId, dstId, (short) 0);

        List<GraphMutation> mutations = List.of(
                new GraphMutation.AddVertex(new VertexData(srcId, Map.of())),
                new GraphMutation.AddVertex(new VertexData(dstId, Map.of())),
                new GraphMutation.AddEdge(new EdgeData(ek, Map.of()))
        );

        BarrierCoordinator barrier = new BarrierCoordinator(List.of(
                new NoOpPartitionNode(), new NoOpPartitionNode()
        ));

        UpdateIngestionRouter router = new UpdateIngestionRouter(cluster, barrier);
        router.ingest(mutations, initialVersion);

        ByteArray newVersion = ByteArray.fromString("v1");
        router.commitAll(newVersion);

        // The edge should be in the partition determined by the strategy
        int edgePartition = strategy.partitionForEdge(srcId, dstId, 2);
        PartitionStore edgeStore = cluster.storeForPartition(edgePartition);
        GraphView view = edgeStore.retrieve(newVersion);
        assertThat(view.edge(srcId, dstId, (short) 0)).isNotNull();
    }

    @Test
    void commitAllTriggersBarrier() {
        List<String> events = new ArrayList<>();

        PartitionNode recordingNode = new PartitionNode() {
            @Override
            public void prepareCommit(ByteArray versionId) {
                events.add("prepare:" + versionId);
            }

            @Override
            public void commit(ByteArray versionId) {
                events.add("commit:" + versionId);
            }
        };

        BarrierCoordinator barrier = new BarrierCoordinator(List.of(recordingNode));

        UpdateIngestionRouter router = new UpdateIngestionRouter(cluster, barrier);

        // Ingest a simple mutation
        List<GraphMutation> mutations = List.of(
                new GraphMutation.AddVertex(new VertexData(100L, Map.of()))
        );
        router.ingest(mutations, initialVersion);

        ByteArray newVersion = ByteArray.fromString("v_barrier");
        router.commitAll(newVersion);

        // Verify barrier was triggered
        assertThat(events).contains("prepare:" + newVersion, "commit:" + newVersion);
    }

    @Test
    void routesRemoveVertexToCorrectPartition() {
        // First add a vertex
        long vid = 42L;
        int partition = strategy.partitionForVertex(vid, 2);

        BarrierCoordinator barrier = new BarrierCoordinator(List.of(
                new NoOpPartitionNode(), new NoOpPartitionNode()
        ));

        // Add vertex
        UpdateIngestionRouter router1 = new UpdateIngestionRouter(cluster, barrier);
        router1.ingest(
                List.of(new GraphMutation.AddVertex(new VertexData(vid, Map.of()))),
                initialVersion
        );
        ByteArray v1 = ByteArray.fromString("v1");
        router1.commitAll(v1);

        // Remove vertex
        UpdateIngestionRouter router2 = new UpdateIngestionRouter(cluster, barrier);
        router2.ingest(
                List.of(new GraphMutation.RemoveVertex(vid)),
                v1
        );
        ByteArray v2 = ByteArray.fromString("v2");
        router2.commitAll(v2);

        PartitionStore store = cluster.storeForPartition(partition);
        GraphView view = store.retrieve(v2);
        assertThat(view.vertex(vid)).isNull();
    }

    /**
     * No-op partition node for tests that don't need barrier recording.
     */
    static class NoOpPartitionNode implements PartitionNode {
        @Override
        public void prepareCommit(ByteArray versionId) {
        }

        @Override
        public void commit(ByteArray versionId) {
        }
    }
}
