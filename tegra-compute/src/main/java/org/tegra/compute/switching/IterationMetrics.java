package org.tegra.compute.switching;

/**
 * Per-iteration metrics recorded during GAS execution.
 * Used as features for the switching classifier.
 *
 * @param activeVertexCount          number of vertices participating in computation
 * @param avgDegreeOfActiveVertices  average degree of active vertices
 * @param activePartitionCount       number of partitions active
 * @param msgsGeneratedPerVertex     messages generated per vertex
 * @param msgsReceivedPerVertex      messages received per vertex
 * @param networkBytesTransferred    bytes transferred over the network
 * @param iterationTimeMs            time taken for the iteration in milliseconds
 */
public record IterationMetrics(
        long activeVertexCount,
        double avgDegreeOfActiveVertices,
        int activePartitionCount,
        double msgsGeneratedPerVertex,
        double msgsReceivedPerVertex,
        long networkBytesTransferred,
        long iterationTimeMs
) {}
