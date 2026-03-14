package org.tegra.compute.gas;

import java.util.Set;

/**
 * GAS (Gather-Apply-Scatter) vertex program interface.
 * Decomposes iterative graph computation into three phases per superstep.
 *
 * @param <V> vertex value type
 * @param <E> edge value type
 * @param <M> message type
 */
public interface VertexProgram<V, E, M> {

    /**
     * Gather: collect information from an adjacent edge triplet.
     *
     * @param context the edge triplet (src, edge, dst)
     * @return a message, or null if no message to produce
     */
    M gather(EdgeTriplet<V, E> context);

    /**
     * Sum: combine two messages. Must be associative and commutative.
     *
     * @param a first message
     * @param b second message
     * @return combined message
     */
    M sum(M a, M b);

    /**
     * Apply: update the vertex value using the gathered/summed message.
     *
     * @param vertexId     the vertex ID
     * @param currentValue current vertex value
     * @param gathered     the aggregated message (null if no messages received)
     * @return the new vertex value
     */
    V apply(long vertexId, V currentValue, M gathered);

    /**
     * Scatter: determine which neighbor vertices should be activated
     * in the next superstep.
     *
     * @param context  the edge triplet
     * @param newValue the newly computed vertex value
     * @return set of vertex IDs to activate, or empty set
     */
    Set<Long> scatter(EdgeTriplet<V, E> context, V newValue);

    /**
     * Specifies which neighbors to gather from.
     */
    EdgeDirection gatherNeighbors();

    /**
     * Specifies which neighbors are activated by scatter.
     */
    EdgeDirection scatterNeighbors();
}
