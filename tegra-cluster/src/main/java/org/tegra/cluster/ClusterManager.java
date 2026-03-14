package org.tegra.cluster;

import org.tegra.cluster.partition.PartitionStrategy;
import org.tegra.store.partition.PartitionStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages cluster topology: node registry, partition assignment, and
 * partition-to-store mapping. Supports both distributed and in-process
 * (multi-partition single-JVM) modes.
 */
public final class ClusterManager {

    private final List<NodeDescriptor> nodes;
    private final PartitionStrategy strategy;
    private final int numPartitions;
    private final Map<Integer, PartitionStore> localStores;

    // Derived: partition ID -> node descriptor
    private final Map<Integer, NodeDescriptor> partitionToNode;

    public ClusterManager(List<NodeDescriptor> nodes,
                          PartitionStrategy strategy,
                          int numPartitions,
                          Map<Integer, PartitionStore> localStores) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        if (strategy == null) {
            throw new IllegalArgumentException("strategy must not be null");
        }
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive");
        }
        if (localStores == null) {
            throw new IllegalArgumentException("localStores must not be null");
        }

        this.nodes = List.copyOf(nodes);
        this.strategy = strategy;
        this.numPartitions = numPartitions;
        this.localStores = Map.copyOf(localStores);

        // Build partition-to-node index
        this.partitionToNode = new HashMap<>();
        for (NodeDescriptor node : this.nodes) {
            for (int partId : node.assignedPartitions()) {
                partitionToNode.put(partId, node);
            }
        }
    }

    /**
     * Starts the cluster manager. Validates partition assignments.
     */
    public void start() {
        // Validate all partitions are assigned
        for (int i = 0; i < numPartitions; i++) {
            if (!partitionToNode.containsKey(i)) {
                throw new IllegalStateException("Partition " + i + " is not assigned to any node");
            }
        }
    }

    /**
     * Stops the cluster manager.
     */
    public void stop() {
        // No-op for in-process mode
    }

    /**
     * Returns the node descriptor responsible for the given partition.
     *
     * @param partitionId the partition ID
     * @return the node descriptor
     */
    public NodeDescriptor nodeForPartition(int partitionId) {
        NodeDescriptor node = partitionToNode.get(partitionId);
        if (node == null) {
            throw new IllegalArgumentException("No node assigned to partition: " + partitionId);
        }
        return node;
    }

    /**
     * Returns the partition ID for the given vertex.
     *
     * @param vertexId the vertex ID
     * @return partition ID in [0, numPartitions)
     */
    public int partitionForVertex(long vertexId) {
        return strategy.partitionForVertex(vertexId, numPartitions);
    }

    /**
     * Returns the partition ID for the given edge.
     *
     * @param srcId source vertex ID
     * @param dstId destination vertex ID
     * @return partition ID in [0, numPartitions)
     */
    public int partitionForEdge(long srcId, long dstId) {
        return strategy.partitionForEdge(srcId, dstId, numPartitions);
    }

    /**
     * Returns the list of partition IDs that have local stores.
     */
    public List<Integer> localPartitions() {
        return new ArrayList<>(localStores.keySet());
    }

    /**
     * Returns the PartitionStore for the given partition.
     *
     * @param partitionId the partition ID
     * @return the partition store
     */
    public PartitionStore storeForPartition(int partitionId) {
        PartitionStore store = localStores.get(partitionId);
        if (store == null) {
            throw new IllegalArgumentException("No local store for partition: " + partitionId);
        }
        return store;
    }

    /**
     * Returns the partition strategy.
     */
    public PartitionStrategy strategy() {
        return strategy;
    }

    /**
     * Returns the total number of partitions.
     */
    public int numPartitions() {
        return numPartitions;
    }

    /**
     * Returns the list of registered nodes.
     */
    public List<NodeDescriptor> nodes() {
        return nodes;
    }
}
