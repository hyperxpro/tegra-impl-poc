package org.tegra.cluster.partition;

/**
 * 2D grid edge partitioning strategy.
 * Edges are assigned by (srcId mod sqrt(P), dstId mod sqrt(P)) mapped to a partition.
 * Vertices are hash-partitioned as usual.
 * <p>
 * This strategy reduces communication by ensuring that edges sharing a source
 * or destination vertex are co-located in fewer partitions.
 */
public final class EdgePartition2D implements PartitionStrategy {

    @Override
    public int partitionForVertex(long vertexId, int numPartitions) {
        return Math.floorMod(Long.hashCode(vertexId), numPartitions);
    }

    @Override
    public int partitionForEdge(long srcId, long dstId, int numPartitions) {
        int sqrtP = (int) Math.ceil(Math.sqrt(numPartitions));
        int row = Math.floorMod(Long.hashCode(srcId), sqrtP);
        int col = Math.floorMod(Long.hashCode(dstId), sqrtP);
        return Math.floorMod(row * sqrtP + col, numPartitions);
    }
}
