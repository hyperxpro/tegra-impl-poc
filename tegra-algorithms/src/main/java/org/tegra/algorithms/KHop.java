package org.tegra.algorithms;

import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;

import java.util.HashSet;
import java.util.Set;

/**
 * K-Hop neighborhood computation.
 * Finds all vertices within k hops of a source vertex.
 * Each vertex holds a set of reachable vertex IDs.
 * The computation runs for exactly k iterations (controlled by maxIterations in GasEngine).
 */
public final class KHop implements VertexProgram<Set<Long>, Object, Set<Long>> {

    private final long sourceVertex;
    private final int k;

    public KHop(long sourceVertex, int k) {
        this.sourceVertex = sourceVertex;
        this.k = k;
    }

    public long sourceVertex() {
        return sourceVertex;
    }

    public int k() {
        return k;
    }

    @Override
    public Set<Long> gather(EdgeTriplet<Set<Long>, Object> context) {
        // Gather reachability sets from neighbors
        Set<Long> result = new HashSet<>();
        Set<Long> srcSet = context.srcValue();
        Set<Long> dstSet = context.dstValue();
        if (srcSet != null) result.addAll(srcSet);
        if (dstSet != null) result.addAll(dstSet);
        return result;
    }

    @Override
    public Set<Long> sum(Set<Long> a, Set<Long> b) {
        Set<Long> result = new HashSet<>(a);
        result.addAll(b);
        return result;
    }

    @Override
    public Set<Long> apply(long vertexId, Set<Long> currentValue, Set<Long> gathered) {
        if (currentValue == null) {
            currentValue = new HashSet<>();
            if (vertexId == sourceVertex) {
                currentValue.add(sourceVertex);
            }
        }
        if (gathered == null) {
            return currentValue;
        }
        // Only absorb from gathered if we are already reachable (non-empty set)
        // or if the gathered set contains reachability info
        Set<Long> result = new HashSet<>(currentValue);
        // If the gathered set contains any reachable vertices, add them plus ourselves
        if (!gathered.isEmpty()) {
            // Only add gathered vertices if we're adjacent to a reachable vertex
            // (the engine only gathers from neighbors, so if any neighbor is reachable, we are too)
            result.addAll(gathered);
            if (!result.isEmpty()) {
                result.add(vertexId);
            }
        }
        return result;
    }

    @Override
    public Set<Long> scatter(EdgeTriplet<Set<Long>, Object> context, Set<Long> newValue) {
        // Activate neighbors if our reachability set grew
        if (newValue != null && !newValue.isEmpty()) {
            return Set.of(context.srcId(), context.dstId());
        }
        return Set.of();
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
