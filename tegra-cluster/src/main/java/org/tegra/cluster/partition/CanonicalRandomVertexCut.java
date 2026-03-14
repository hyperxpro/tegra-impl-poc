package org.tegra.cluster.partition;

/**
 * Canonical random vertex-cut partitioning strategy.
 * Edges are assigned by hash(min(srcId, dstId), max(srcId, dstId)) % numPartitions.
 * This ensures that edges (u, v) and (v, u) are co-located in the same partition.
 * Vertices are hash-partitioned as usual.
 */
public final class CanonicalRandomVertexCut implements PartitionStrategy {

    @Override
    public int partitionForVertex(long vertexId, int numPartitions) {
        return Math.floorMod(Long.hashCode(vertexId), numPartitions);
    }

    @Override
    public int partitionForEdge(long srcId, long dstId, int numPartitions) {
        long lo = Math.min(srcId, dstId);
        long hi = Math.max(srcId, dstId);
        long combined = lo * 31L + hi;
        return Math.floorMod(Long.hashCode(combined), numPartitions);
    }
}
