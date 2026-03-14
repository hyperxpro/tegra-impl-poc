package org.tegra.algorithms;

import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Label Propagation for community detection.
 * Each vertex starts with its own ID as label, then iteratively
 * adopts the most frequent label among its neighbors.
 * <p>
 * Uses BOTH direction gather, collecting frequency maps from edge triplets.
 * For each triplet, only the neighbor's label is counted (not the active vertex's own label).
 * Since with BOTH direction, each undirected edge produces two triplets
 * (one from outEdges, one from inEdges), we use srcValue from inEdge triplets
 * and dstValue from outEdge triplets. Since we cannot distinguish these in the
 * gather method, we take the simpler approach of gathering from IN direction only,
 * where srcValue is always the neighbor's label.
 */
public final class LabelPropagation implements VertexProgram<Long, Object, Map<Long, Long>> {

    @Override
    public Map<Long, Long> gather(EdgeTriplet<Long, Object> context) {
        // With IN direction: the active vertex is dst, neighbor is src.
        // Count the neighbor's (src) label.
        Map<Long, Long> freq = new HashMap<>();
        Long srcLabel = context.srcValue();
        if (srcLabel != null) {
            freq.put(srcLabel, 1L);
        }
        return freq;
    }

    @Override
    public Map<Long, Long> sum(Map<Long, Long> a, Map<Long, Long> b) {
        Map<Long, Long> result = new HashMap<>(a);
        b.forEach((label, count) -> result.merge(label, count, Long::sum));
        return result;
    }

    @Override
    public Long apply(long vertexId, Long currentValue, Map<Long, Long> gathered) {
        if (currentValue == null) {
            currentValue = vertexId;
        }
        if (gathered == null || gathered.isEmpty()) {
            return currentValue;
        }
        // Adopt the most frequent label; break ties by choosing the smaller label
        long bestLabel = currentValue;
        long bestCount = 0;
        for (Map.Entry<Long, Long> entry : gathered.entrySet()) {
            if (entry.getValue() > bestCount ||
                    (entry.getValue() == bestCount && entry.getKey() < bestLabel)) {
                bestLabel = entry.getKey();
                bestCount = entry.getValue();
            }
        }
        return bestLabel;
    }

    @Override
    public Set<Long> scatter(EdgeTriplet<Long, Object> context, Long newValue) {
        // Activate the neighbor (dst of outgoing edges)
        return Set.of(context.dstId());
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
