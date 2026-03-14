package org.tegra.cluster.ingestion;

import org.tegra.cluster.ClusterManager;
import org.tegra.cluster.barrier.BarrierCoordinator;
import org.tegra.cluster.barrier.PartitionNode;
import org.tegra.serde.EdgeData;
import org.tegra.serde.VertexData;
import org.tegra.store.log.GraphMutation;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Routes graph mutations to the correct partitions based on the cluster's
 * partition strategy. After ingestion, coordinates a distributed commit
 * across all partitions.
 */
public final class UpdateIngestionRouter {

    private final ClusterManager cluster;
    private final BarrierCoordinator barrierCoordinator;

    // Per-partition working versions for the current batch
    private final Map<Integer, WorkingVersion> workingVersions;

    public UpdateIngestionRouter(ClusterManager cluster, BarrierCoordinator barrierCoordinator) {
        this.cluster = cluster;
        this.barrierCoordinator = barrierCoordinator;
        this.workingVersions = new HashMap<>();
    }

    /**
     * Routes each mutation to the correct partition and applies it to
     * the partition's working version.
     *
     * @param mutations   the list of mutations to ingest
     * @param sourceVersion the version to branch from for working versions
     */
    public void ingest(List<GraphMutation> mutations, ByteArray sourceVersion) {
        for (GraphMutation mutation : mutations) {
            int partitionId = partitionForMutation(mutation);
            WorkingVersion wv = getOrCreateWorkingVersion(partitionId, sourceVersion);
            applyMutation(wv, mutation);
        }
    }

    /**
     * Triggers a coordinated commit across all partitions that have pending changes.
     *
     * @param versionId the version ID for the new snapshot
     * @return the committed version ID
     */
    public ByteArray commitAll(ByteArray versionId) {
        // Commit each partition's working version
        for (Map.Entry<Integer, WorkingVersion> entry : workingVersions.entrySet()) {
            int pid = entry.getKey();
            WorkingVersion wv = entry.getValue();
            PartitionStore store = cluster.storeForPartition(pid);
            store.commit(wv, versionId);
        }

        // Also ensure partitions without changes have this version
        // by creating it from their current state
        for (int pid : cluster.localPartitions()) {
            if (!workingVersions.containsKey(pid)) {
                PartitionStore store = cluster.storeForPartition(pid);
                // Branch and immediately commit to create the new version
                try {
                    WorkingVersion wv = store.branch(workingVersions.values().iterator().next().sourceVersionId());
                    store.commit(wv, versionId);
                } catch (Exception e) {
                    // If source version doesn't exist in this partition, skip
                }
            }
        }

        workingVersions.clear();

        return barrierCoordinator.coordinateCommit(versionId).join();
    }

    /**
     * Determines which partition a mutation should be routed to.
     */
    private int partitionForMutation(GraphMutation mutation) {
        return switch (mutation) {
            case GraphMutation.AddVertex addVertex ->
                    cluster.partitionForVertex(addVertex.vertexData().vertexId());
            case GraphMutation.RemoveVertex removeVertex ->
                    cluster.partitionForVertex(removeVertex.vertexId());
            case GraphMutation.AddEdge addEdge ->
                    cluster.partitionForEdge(
                            addEdge.edgeData().edgeKey().srcId(),
                            addEdge.edgeData().edgeKey().dstId());
            case GraphMutation.RemoveEdge removeEdge ->
                    cluster.partitionForEdge(removeEdge.srcId(), removeEdge.dstId());
            case GraphMutation.UpdateVertexProperty updateVP ->
                    cluster.partitionForVertex(updateVP.vertexId());
            case GraphMutation.UpdateEdgeProperty updateEP ->
                    cluster.partitionForEdge(updateEP.srcId(), updateEP.dstId());
        };
    }

    /**
     * Gets or creates a working version for the given partition.
     */
    private WorkingVersion getOrCreateWorkingVersion(int partitionId, ByteArray sourceVersion) {
        return workingVersions.computeIfAbsent(partitionId, pid -> {
            PartitionStore store = cluster.storeForPartition(pid);
            return store.branch(sourceVersion);
        });
    }

    /**
     * Applies a single mutation to a working version.
     */
    private void applyMutation(WorkingVersion wv, GraphMutation mutation) {
        switch (mutation) {
            case GraphMutation.AddVertex addVertex ->
                    wv.putVertex(addVertex.vertexData().vertexId(), addVertex.vertexData());
            case GraphMutation.RemoveVertex removeVertex ->
                    wv.removeVertex(removeVertex.vertexId());
            case GraphMutation.AddEdge addEdge -> {
                EdgeData ed = addEdge.edgeData();
                wv.putEdge(ed.edgeKey().srcId(), ed.edgeKey().dstId(),
                        ed.edgeKey().discriminator(), ed);
            }
            case GraphMutation.RemoveEdge removeEdge ->
                    wv.removeEdge(removeEdge.srcId(), removeEdge.dstId(), removeEdge.discriminator());
            case GraphMutation.UpdateVertexProperty updateVP -> {
                VertexData existing = wv.getVertex(updateVP.vertexId());
                if (existing != null) {
                    var newProps = new HashMap<>(existing.properties());
                    newProps.put(updateVP.propertyKey(), updateVP.value());
                    wv.putVertex(updateVP.vertexId(), new VertexData(updateVP.vertexId(), newProps));
                }
            }
            case GraphMutation.UpdateEdgeProperty updateEP -> {
                EdgeData existing = wv.getEdge(updateEP.srcId(), updateEP.dstId(), updateEP.discriminator());
                if (existing != null) {
                    var newProps = new HashMap<>(existing.properties());
                    newProps.put(updateEP.propertyKey(), updateEP.value());
                    wv.putEdge(updateEP.srcId(), updateEP.dstId(), updateEP.discriminator(),
                            new EdgeData(existing.edgeKey(), newProps));
                }
            }
        }
    }
}
