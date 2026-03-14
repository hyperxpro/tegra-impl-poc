package org.tegra.algorithms;

import org.tegra.api.EdgeDirection;
import org.tegra.compute.gas.VertexProgram;

/**
 * Connected components via label propagation in the GAS model.
 * <p>
 * Each vertex is initialized with its own ID as its component label.
 * In each superstep, vertices adopt the minimum label among themselves
 * and their neighbors, converging when labels stabilize.
 *
 * @param <E> edge property type (unused)
 */
public final class ConnectedComponents<E> implements VertexProgram<Long, E, Long> {

    @Override
    public String name() {
        return "CC";
    }

    @Override
    public EdgeDirection gatherDirection() {
        return EdgeDirection.BOTH;
    }

    @Override
    public EdgeDirection scatterDirection() {
        return EdgeDirection.BOTH;
    }

    @Override
    public Long gather(Long vertexValue, E edgeValue, Long neighborValue) {
        return neighborValue;
    }

    @Override
    public Long sum(Long a, Long b) {
        return Math.min(a, b);
    }

    @Override
    public Long apply(Long currentValue, Long gathered) {
        return Math.min(currentValue, gathered);
    }

    @Override
    public boolean scatter(Long updatedValue, Long oldValue, E edgeValue) {
        return !updatedValue.equals(oldValue);
    }

    @Override
    public Long identity() {
        return Long.MAX_VALUE;
    }
}
