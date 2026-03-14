package org.tegra.cluster.partition;

/**
 * Random vertex-cut partitioning strategy.
 * Edges are assigned by hash(srcId, dstId) % numPartitions.
 * Vertices are hash-partitioned as usual.
 */
public final class RandomVertexCut implements PartitionStrategy {

    @Override
    public int partitionForVertex(long vertexId, int numPartitions) {
        return Math.floorMod(Long.hashCode(vertexId), numPartitions);
    }

    @Override
    public int partitionForEdge(long srcId, long dstId, int numPartitions) {
        // Combine srcId and dstId into a single hash
        long combined = srcId * 31L + dstId;
        return Math.floorMod(Long.hashCode(combined), numPartitions);
    }
}
