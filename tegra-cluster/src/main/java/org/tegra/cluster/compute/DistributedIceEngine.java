package org.tegra.cluster.compute;

import org.tegra.cluster.ClusterManager;
import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.VertexProgram;
import org.tegra.compute.ice.DiffEngine;
import org.tegra.compute.ice.NeighborhoodExpander;
import org.tegra.api.Delta;
import org.tegra.api.SubgraphView;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.version.ByteArray;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Distributed incremental computation engine.
 * Implements ICE (Incremental Computation by Entity expansion)
 * across multiple partitions.
 * <p>
 * Steps:
 * 1. Each partition computes its local diff.
 * 2. Expand affected vertices (may cross partitions).
 * 3. Run GAS on affected subgraph with cross-partition messaging.
 * 4. Copy previous results for unaffected vertices.
 */
public final class DistributedIceEngine {

    private final DistributedGasEngine gasEngine;
    private final ClusterManager cluster;
    private final DiffEngine diffEngine;
    private final NeighborhoodExpander expander;

    public DistributedIceEngine(DistributedGasEngine gasEngine, ClusterManager cluster) {
        this.gasEngine = gasEngine;
        this.cluster = cluster;
        this.diffEngine = new DiffEngine();
        this.expander = new NeighborhoodExpander();
    }

    /**
     * Incremental Pregel across partitions.
     *
     * @param snapshotId      the current snapshot version
     * @param prevSnapshotId  the previous snapshot version
     * @param previousResults previous computation results (vertex ID to value)
     * @param program         the vertex program
     * @param maxIterations   maximum number of supersteps
     * @param <V>             vertex value type
     * @param <E>             edge value type
     * @param <M>             message type
     * @return map of partition ID to (vertex ID to computed value)
     */
    public <V, E, M> Map<Integer, Map<Long, V>> incPregel(
            ByteArray snapshotId,
            ByteArray prevSnapshotId,
            Map<Long, V> previousResults,
            VertexProgram<V, E, M> program,
            int maxIterations) {

        List<Integer> partitionIds = cluster.localPartitions();

        // Step 1: Compute diffs per partition and collect affected vertices
        Set<Long> allAffected = new HashSet<>();
        Set<Long> allRemoved = new HashSet<>();
        boolean anyChanges = false;

        for (int pid : partitionIds) {
            GraphView currentGraph = cluster.storeForPartition(pid).retrieve(snapshotId);
            GraphView previousGraph = cluster.storeForPartition(pid).retrieve(prevSnapshotId);

            Delta delta = diffEngine.computeDiff(currentGraph, previousGraph);
            if (!delta.isEmpty()) {
                anyChanges = true;
                allAffected.addAll(delta.affectedVertices());
                allRemoved.addAll(delta.removedVertices());
            }
        }

        // If no changes, return previous results distributed across partitions
        if (!anyChanges) {
            Map<Integer, Map<Long, V>> result = new HashMap<>();
            for (int pid : partitionIds) {
                GraphView graph = cluster.storeForPartition(pid).retrieve(snapshotId);
                Set<Long> vertexIds = collectVertexIds(graph);
                Map<Long, V> partValues = new HashMap<>();
                for (long vid : vertexIds) {
                    if (previousResults.containsKey(vid)) {
                        partValues.put(vid, previousResults.get(vid));
                    }
                }
                result.put(pid, partValues);
            }
            return result;
        }

        // Step 2: Expand affected vertices by 1-hop per partition
        Map<Integer, Set<Long>> partitionActiveIds = new HashMap<>();
        for (int pid : partitionIds) {
            GraphView currentGraph = cluster.storeForPartition(pid).retrieve(snapshotId);
            Set<Long> localVertexIds = collectVertexIds(currentGraph);

            // Find affected vertices local to this partition
            Set<Long> localAffected = new HashSet<>();
            for (long vid : allAffected) {
                if (localVertexIds.contains(vid)) {
                    localAffected.add(vid);
                }
            }

            // Expand by 1-hop
            if (!localAffected.isEmpty()) {
                SubgraphView subgraphView = expander.expand(
                        localAffected, currentGraph, program.gatherNeighbors());
                Set<Long> expanded = new HashSet<>(subgraphView.activeVertexIds());
                expanded.addAll(subgraphView.boundaryVertexIds());
                partitionActiveIds.put(pid, expanded);
            } else {
                partitionActiveIds.put(pid, new HashSet<>());
            }
        }

        // Step 3: Prepare current values (start with previous results, remove deleted)
        Map<Long, V> currentValues = new HashMap<>(previousResults);
        for (long vid : allRemoved) {
            currentValues.remove(vid);
        }

        // Step 4: Run distributed GAS on the affected subgraph
        Map<Integer, Map<Long, V>> computedResults = gasEngine.executeOnSubgraph(
                snapshotId, program, currentValues, partitionActiveIds, maxIterations);

        // Step 5: Fill in unaffected vertices with previous results
        for (int pid : partitionIds) {
            GraphView graph = cluster.storeForPartition(pid).retrieve(snapshotId);
            Set<Long> vertexIds = collectVertexIds(graph);
            Map<Long, V> partValues = computedResults.get(pid);
            if (partValues == null) {
                partValues = new HashMap<>();
                computedResults.put(pid, partValues);
            }
            for (long vid : vertexIds) {
                if (!partValues.containsKey(vid) && previousResults.containsKey(vid)) {
                    partValues.put(vid, previousResults.get(vid));
                }
            }
        }

        return computedResults;
    }

    private Set<Long> collectVertexIds(GraphView graph) {
        Set<Long> ids = new HashSet<>();
        Iterator<VertexData> it = graph.vertices();
        while (it.hasNext()) {
            ids.add(it.next().vertexId());
        }
        return ids;
    }
}
