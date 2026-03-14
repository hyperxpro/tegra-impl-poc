package org.tegra.benchmark.workload;

import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.store.GraphView;
import org.tegra.store.log.GraphMutation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Generates sequences of graph mutations that simulate temporal graph evolution.
 * Used by the benchmark harness to create realistic workloads.
 */
public final class WorkloadGenerator {

    private final Random rng;

    public WorkloadGenerator() {
        this(42L);
    }

    public WorkloadGenerator(long seed) {
        this.rng = new Random(seed);
    }

    /**
     * Generates a sequence of mutation batches for the given base graph.
     * Each batch represents the mutations to apply for one snapshot.
     *
     * @param baseGraph    the starting graph
     * @param mutationRate fraction of edges to mutate per snapshot (e.g., 0.01 for 1%)
     * @param numSnapshots number of snapshot batches to generate
     * @return list of mutation batches, one per snapshot
     */
    public List<List<GraphMutation>> generateEvolution(
            GraphView baseGraph, double mutationRate, int numSnapshots) {
        return generateEvolution(baseGraph, mutationRate, numSnapshots, MutationType.MIXED);
    }

    /**
     * Generates a sequence of mutation batches with a specified mutation type.
     *
     * @param baseGraph    the starting graph
     * @param mutationRate fraction of edges to mutate per snapshot
     * @param numSnapshots number of snapshot batches to generate
     * @param type         the type of mutations to generate
     * @return list of mutation batches, one per snapshot
     */
    public List<List<GraphMutation>> generateEvolution(
            GraphView baseGraph, double mutationRate, int numSnapshots, MutationType type) {

        // Collect existing edges
        List<EdgeKey> existingEdges = new ArrayList<>();
        Iterator<EdgeData> edgeIt = baseGraph.edges();
        while (edgeIt.hasNext()) {
            existingEdges.add(edgeIt.next().edgeKey());
        }

        // Collect vertex IDs for generating new edges
        List<Long> vertexIds = new ArrayList<>();
        var vertexIt = baseGraph.vertices();
        while (vertexIt.hasNext()) {
            vertexIds.add(vertexIt.next().vertexId());
        }

        long edgeCount = existingEdges.size();
        int mutationsPerSnapshot = Math.max(1, (int) (edgeCount * mutationRate));

        // Track current edge state for accurate removal
        List<EdgeKey> currentEdges = new ArrayList<>(existingEdges);
        List<List<GraphMutation>> allBatches = new ArrayList<>();

        for (int s = 0; s < numSnapshots; s++) {
            List<GraphMutation> batch = new ArrayList<>();

            switch (type) {
                case ADDITIONS_ONLY -> {
                    for (int m = 0; m < mutationsPerSnapshot; m++) {
                        EdgeKey newEdge = generateNewEdge(vertexIds, currentEdges);
                        batch.add(new GraphMutation.AddEdge(
                                new EdgeData(newEdge, Map.of())));
                        currentEdges.add(newEdge);
                    }
                }
                case DELETIONS_ONLY -> {
                    int removals = Math.min(mutationsPerSnapshot, currentEdges.size());
                    // Shuffle for random removal order
                    List<Integer> indices = new ArrayList<>();
                    for (int i = 0; i < currentEdges.size(); i++) {
                        indices.add(i);
                    }
                    Collections.shuffle(indices, rng);
                    List<EdgeKey> toRemove = new ArrayList<>();
                    for (int m = 0; m < removals; m++) {
                        EdgeKey ek = currentEdges.get(indices.get(m));
                        batch.add(new GraphMutation.RemoveEdge(
                                ek.srcId(), ek.dstId(), ek.discriminator()));
                        toRemove.add(ek);
                    }
                    currentEdges.removeAll(toRemove);
                }
                case MIXED -> {
                    int halfMutations = Math.max(1, mutationsPerSnapshot / 2);

                    // Additions
                    for (int m = 0; m < halfMutations; m++) {
                        EdgeKey newEdge = generateNewEdge(vertexIds, currentEdges);
                        batch.add(new GraphMutation.AddEdge(
                                new EdgeData(newEdge, Map.of())));
                        currentEdges.add(newEdge);
                    }

                    // Removals
                    int removals = Math.min(halfMutations, currentEdges.size());
                    List<Integer> indices = new ArrayList<>();
                    for (int i = 0; i < currentEdges.size(); i++) {
                        indices.add(i);
                    }
                    Collections.shuffle(indices, rng);
                    List<EdgeKey> toRemove = new ArrayList<>();
                    for (int m = 0; m < removals; m++) {
                        EdgeKey ek = currentEdges.get(indices.get(m));
                        batch.add(new GraphMutation.RemoveEdge(
                                ek.srcId(), ek.dstId(), ek.discriminator()));
                        toRemove.add(ek);
                    }
                    currentEdges.removeAll(toRemove);
                }
            }

            allBatches.add(batch);
        }

        return allBatches;
    }

    /**
     * Generates a new random edge that does not already exist.
     */
    private EdgeKey generateNewEdge(List<Long> vertexIds, List<EdgeKey> currentEdges) {
        if (vertexIds.size() < 2) {
            throw new IllegalStateException("Need at least 2 vertices to generate edges");
        }

        // Simple approach: generate random (src, dst) pairs until we find a new one
        for (int attempt = 0; attempt < 100; attempt++) {
            long srcId = vertexIds.get(rng.nextInt(vertexIds.size()));
            long dstId = vertexIds.get(rng.nextInt(vertexIds.size()));

            if (srcId == dstId) {
                continue;
            }

            EdgeKey candidate = new EdgeKey(srcId, dstId, (short) 0);
            // Check for duplicates (linear scan is acceptable for benchmark setup)
            boolean exists = false;
            for (EdgeKey existing : currentEdges) {
                if (existing.srcId() == candidate.srcId()
                        && existing.dstId() == candidate.dstId()
                        && existing.discriminator() == candidate.discriminator()) {
                    exists = true;
                    break;
                }
            }

            if (!exists) {
                return candidate;
            }
        }

        // Fallback: just return a random edge even if duplicate
        long srcId = vertexIds.get(rng.nextInt(vertexIds.size()));
        long dstId = vertexIds.get(rng.nextInt(vertexIds.size()));
        if (srcId == dstId) {
            dstId = vertexIds.get((rng.nextInt(vertexIds.size() - 1) + 1) % vertexIds.size());
        }
        return new EdgeKey(srcId, dstId, (short) 0);
    }
}
