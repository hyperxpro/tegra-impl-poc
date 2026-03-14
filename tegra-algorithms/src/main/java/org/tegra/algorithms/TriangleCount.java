package org.tegra.algorithms;

import org.tegra.api.GraphAlgorithm;
import org.tegra.api.GraphSnapshot;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Triangle counting algorithm implemented directly against a graph snapshot.
 * <p>
 * For each vertex, counts the number of triangles it participates in.
 * A triangle is a set of three mutually connected vertices.
 * <p>
 * This algorithm does not fit the GAS model cleanly, so it operates
 * directly on the {@link GraphSnapshot} via the {@link GraphAlgorithm} interface.
 *
 * @param <V> vertex property type (unused)
 * @param <E> edge property type (unused)
 */
public final class TriangleCount<V, E> implements GraphAlgorithm<V, E, Map<Long, Long>> {

    @Override
    public Map<Long, Long> execute(GraphSnapshot<V, E> snapshot) {
        Map<Long, Set<Long>> neighborSets = buildNeighborSets(snapshot);

        Map<Long, Long> counts = new HashMap<>();
        for (long v : neighborSets.keySet()) {
            counts.put(v, 0L);
        }

        // Enumerate triangles using canonical ordering: for each edge (u, v) with u < v,
        // find common neighbors w with w > v, yielding unique triples (u, v, w) where u < v < w.
        // Edges (u, w) with u < w < v also find their third vertex v > w in the intersection,
        // so every triangle is counted exactly once across all edge iterations.
        for (Map.Entry<Long, Set<Long>> entry : neighborSets.entrySet()) {
            long u = entry.getKey();
            Set<Long> neighborsU = entry.getValue();
            for (long v : neighborsU) {
                if (u < v) {
                    Set<Long> neighborsV = neighborSets.getOrDefault(v, Set.of());
                    for (long w : neighborsU) {
                        if (w > v && neighborsV.contains(w)) {
                            counts.merge(u, 1L, Long::sum);
                            counts.merge(v, 1L, Long::sum);
                            counts.merge(w, 1L, Long::sum);
                        }
                    }
                }
            }
        }

        return counts;
    }

    private Map<Long, Set<Long>> buildNeighborSets(GraphSnapshot<V, E> snapshot) {
        Map<Long, Set<Long>> neighbors = new HashMap<>();

        snapshot.vertices().forEach(v -> neighbors.put(v.id(), new HashSet<>()));

        snapshot.edges().forEach(edge -> {
            neighbors.computeIfAbsent(edge.src(), k -> new HashSet<>()).add(edge.dst());
            neighbors.computeIfAbsent(edge.dst(), k -> new HashSet<>()).add(edge.src());
        });

        return neighbors;
    }
}
