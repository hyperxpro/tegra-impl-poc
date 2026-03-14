package org.tegra.algorithms;

import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;

import java.util.Set;

/**
 * Connected Components via label propagation.
 * Each vertex starts with its own ID as its label, then iteratively
 * adopts the minimum label among itself and its neighbors.
 * Converges when no labels change.
 */
public final class ConnectedComponents implements VertexProgram<Long, Object, Long> {

    @Override
    public Long gather(EdgeTriplet<Long, Object> context) {
        // For BOTH direction, we need to gather the neighbor's value.
        // When this triplet comes from outEdges(v), v is the src, neighbor is dst.
        // When this triplet comes from inEdges(v), v is the dst, neighbor is src.
        // We want the neighbor's label in both cases.
        // The engine builds triplets for the active vertex, so we need both ends.
        // We'll just return the minimum of src and dst values to be safe —
        // but actually we need the *neighbor's* value. Since the engine calls
        // gather for each triplet of the active vertex, we return the minimum
        // of both endpoints (the active vertex will also contribute via apply).
        Long src = context.srcValue();
        Long dst = context.dstValue();
        long minVal = Long.MAX_VALUE;
        if (src != null) minVal = Math.min(minVal, src);
        if (dst != null) minVal = Math.min(minVal, dst);
        return minVal;
    }

    @Override
    public Long sum(Long a, Long b) {
        return Math.min(a, b);
    }

    @Override
    public Long apply(long vertexId, Long currentValue, Long gathered) {
        if (currentValue == null) {
            currentValue = vertexId;
        }
        if (gathered == null) {
            return currentValue;
        }
        return Math.min(currentValue, gathered);
    }

    @Override
    public Set<Long> scatter(EdgeTriplet<Long, Object> context, Long newValue) {
        // Activate the neighbor vertex (the other end of the edge).
        // For outEdges: neighbor is dst. For inEdges: neighbor is src.
        // We activate both endpoints so whichever is the neighbor gets activated.
        return Set.of(context.srcId(), context.dstId());
    }

    @Override
    public EdgeDirection gatherNeighbors() {
        return EdgeDirection.BOTH;
    }

    @Override
    public EdgeDirection scatterNeighbors() {
        return EdgeDirection.BOTH;
    }
}
