package org.tegra.cluster.partition;

/**
 * Strategy for assigning vertices and edges to partitions.
 * Implementations determine how the graph is distributed across cluster nodes.
 */
public interface PartitionStrategy {

    /**
     * Returns the partition ID for the given vertex.
     *
     * @param vertexId      the vertex identifier
     * @param numPartitions the total number of partitions
     * @return partition ID in [0, numPartitions)
     */
    int partitionForVertex(long vertexId, int numPartitions);

    /**
     * Returns the partition ID for the given edge.
     *
     * @param srcId         source vertex ID
     * @param dstId         destination vertex ID
     * @param numPartitions the total number of partitions
     * @return partition ID in [0, numPartitions)
     */
    int partitionForEdge(long srcId, long dstId, int numPartitions);
}
