package org.tegra.cluster.partition;

/**
 * Hash-based partitioning strategy.
 * Both vertices and edges are assigned based on hash(vertexId) % numPartitions.
 * For edges, the source vertex ID determines the partition.
 */
public final class HashPartitioning implements PartitionStrategy {

    @Override
    public int partitionForVertex(long vertexId, int numPartitions) {
        return Math.floorMod(Long.hashCode(vertexId), numPartitions);
    }

    @Override
    public int partitionForEdge(long srcId, long dstId, int numPartitions) {
        // Edge follows the source vertex partition
        return partitionForVertex(srcId, numPartitions);
    }
}
