package org.tegra.algorithms;

import org.tegra.api.EdgeDirection;
import org.tegra.compute.gas.VertexProgram;

/**
 * Simplified label propagation for community detection in the GAS model.
 * <p>
 * Each vertex is initialized with its own ID as its label. In each superstep,
 * a vertex adopts the minimum label among its neighbors (simplified from
 * majority-vote to minimum-label for determinism and GAS compatibility).
 * <p>
 * This produces the same result as {@link ConnectedComponents} but is
 * semantically intended for community detection use cases.
 *
 * @param <E> edge property type (unused)
 */
public final class LabelPropagation<E> implements VertexProgram<Long, E, Long> {

    @Override
    public String name() {
        return "LabelProp";
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
