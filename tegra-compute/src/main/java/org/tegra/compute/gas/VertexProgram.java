package org.tegra.compute.gas;

import org.tegra.api.EdgeDirection;

import java.util.Objects;

/**
 * Interface for GAS (Gather-Apply-Scatter) vertex-centric programs.
 * <p>
 * Implementations define the three phases of computation:
 * <ul>
 *   <li><b>Gather</b> — collect a message from each neighbor along the gather direction</li>
 *   <li><b>Apply</b> — update the vertex value using the aggregated gathered messages</li>
 *   <li><b>Scatter</b> — decide whether each neighbor should be activated next iteration</li>
 * </ul>
 *
 * @param <V> vertex value type
 * @param <E> edge value type
 * @param <M> message type (must form a commutative monoid under {@link #sum})
 */
public interface VertexProgram<V, E, M> {

    /** Human-readable name for this program (used in logging / snapshot IDs). */
    String name();

    /** Direction from which to gather messages. */
    EdgeDirection gatherDirection();

    /** Direction along which to scatter activation signals. */
    EdgeDirection scatterDirection();

    /**
     * Produce a message from a single edge during the gather phase.
     *
     * @param vertexValue   current value of the gathering vertex
     * @param edgeValue     properties on the edge being traversed
     * @param neighborValue value of the neighbor at the other end
     * @return message to be aggregated via {@link #sum}
     */
    M gather(V vertexValue, E edgeValue, V neighborValue);

    /**
     * Associative, commutative aggregation of two messages.
     */
    M sum(M a, M b);

    /**
     * Apply the aggregated message to produce a new vertex value.
     *
     * @param currentValue current vertex value
     * @param gathered     aggregated message (or {@link #identity()} if no neighbors)
     * @return updated vertex value
     */
    V apply(V currentValue, M gathered);

    /**
     * Decide whether to activate a neighbor during the scatter phase.
     *
     * @param updatedValue new value of the scattering vertex
     * @param oldValue     previous value of the scattering vertex
     * @param edgeValue    properties on the edge toward the neighbor
     * @return {@code true} to activate the neighbor in the next iteration
     */
    boolean scatter(V updatedValue, V oldValue, E edgeValue);

    /** Identity element for the message monoid ({@link #sum} with identity returns the other operand). */
    M identity();

    /** Maximum number of supersteps before forced termination. */
    default int maxIterations() {
        return 100;
    }

    /** Per-vertex convergence test; default is value equality. */
    default boolean hasConverged(V oldValue, V newValue) {
        return Objects.equals(oldValue, newValue);
    }
}
