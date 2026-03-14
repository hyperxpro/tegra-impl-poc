package org.tegra.algorithms;

import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;

import java.util.Set;

/**
 * Breadth-First Search computing hop distances from a source vertex.
 * Each vertex stores its distance (Integer) from the source.
 * Source vertex starts at 0, all others at Integer.MAX_VALUE.
 */
public final class BreadthFirstSearch implements VertexProgram<Integer, Object, Integer> {

    private final long sourceVertex;

    public BreadthFirstSearch(long sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    public long sourceVertex() {
        return sourceVertex;
    }

    @Override
    public Integer gather(EdgeTriplet<Integer, Object> context) {
        // Gather from incoming edges: neighbor distance + 1
        Integer srcDist = context.srcValue();
        if (srcDist == null || srcDist == Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return srcDist + 1;
    }

    @Override
    public Integer sum(Integer a, Integer b) {
        return Math.min(a, b);
    }

    @Override
    public Integer apply(long vertexId, Integer currentValue, Integer gathered) {
        if (currentValue == null) {
            currentValue = (vertexId == sourceVertex) ? 0 : Integer.MAX_VALUE;
        }
        if (gathered == null) {
            return currentValue;
        }
        return Math.min(currentValue, gathered);
    }

    @Override
    public Set<Long> scatter(EdgeTriplet<Integer, Object> context, Integer newValue) {
        // Activate the destination if it hasn't been reached yet or can be improved
        Integer dstDist = context.dstValue();
        if (dstDist == null || newValue + 1 < dstDist) {
            return Set.of(context.dstId());
        }
        return Set.of();
    }

    @Override
    public EdgeDirection gatherNeighbors() {
        return EdgeDirection.IN;
    }

    @Override
    public EdgeDirection scatterNeighbors() {
        return EdgeDirection.OUT;
    }
}
