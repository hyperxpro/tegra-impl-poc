package org.tegra.cluster.compute;

import org.tegra.cluster.ClusterManager;
import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;
import org.tegra.serde.EdgeData;
import org.tegra.serde.VertexData;
import org.tegra.store.GraphView;
import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Distributed GAS engine that executes vertex programs across multiple partitions.
 * <p>
 * In a distributed graph, edges are partitioned by source vertex (HashPartitioning)
 * or other strategies. This means IN-edges for a vertex may be stored in a different
 * partition than the vertex itself. The engine handles cross-partition edge traversal
 * by querying all partition graphs when building edge triplets.
 * <p>
 * Each iteration:
 * 1. Each partition runs gather on active vertices, scanning all partitions for edges.
 * 2. Apply is executed locally on each partition.
 * 3. Scatter activates vertices (possibly on remote partitions), scanning all partitions.
 * 4. Barrier synchronization between iterations.
 */
public final class DistributedGasEngine {

    private final ClusterManager cluster;

    public DistributedGasEngine(ClusterManager cluster) {
        this.cluster = cluster;
    }

    /**
     * Execute a vertex program across all partitions.
     *
     * @param snapshotId    the snapshot version to compute on
     * @param program       the vertex program
     * @param initialValues initial vertex values (vertex ID to value)
     * @param maxIterations maximum number of supersteps
     * @param <V>           vertex value type
     * @param <E>           edge value type
     * @param <M>           message type
     * @return map of partition ID to (vertex ID to final value)
     */
    public <V, E, M> Map<Integer, Map<Long, V>> execute(
            ByteArray snapshotId,
            VertexProgram<V, E, M> program,
            Map<Long, V> initialValues,
            int maxIterations) {

        List<Integer> partitionIds = cluster.localPartitions();

        // Load all partition graphs
        List<GraphView> allGraphs = new ArrayList<>();
        Map<Integer, GraphView> partitionGraphs = new HashMap<>();
        Map<Integer, Set<Long>> partitionAllVertexIds = new HashMap<>();
        Map<Integer, Map<Long, V>> partitionValues = new HashMap<>();
        Map<Integer, Set<Long>> partitionActiveVertices = new HashMap<>();

        for (int pid : partitionIds) {
            GraphView graph = cluster.storeForPartition(pid).retrieve(snapshotId);
            allGraphs.add(graph);
            partitionGraphs.put(pid, graph);

            Set<Long> allVertexIds = collectVertexIds(graph);
            partitionAllVertexIds.put(pid, allVertexIds);

            Map<Long, V> values = new HashMap<>();
            for (long vid : allVertexIds) {
                if (initialValues.containsKey(vid)) {
                    values.put(vid, initialValues.get(vid));
                }
            }
            partitionValues.put(pid, values);
            partitionActiveVertices.put(pid, new HashSet<>(allVertexIds));
        }

        // Superstep loop
        for (int iteration = 0; iteration < maxIterations; iteration++) {
            boolean anyActive = partitionIds.stream()
                    .anyMatch(pid -> !partitionActiveVertices.get(pid).isEmpty());
            if (!anyActive) {
                break;
            }

            // Build global values view
            Map<Long, V> globalValues = new HashMap<>();
            for (Map<Long, V> pv : partitionValues.values()) {
                globalValues.putAll(pv);
            }

            // === GATHER + SUM ===
            // For each active vertex, gather from ALL partition graphs
            Map<Integer, Map<Long, M>> partitionGathered = new HashMap<>();
            for (int pid : partitionIds) {
                Set<Long> activeVertices = partitionActiveVertices.get(pid);
                Map<Long, M> gathered = gatherAndSumAcrossPartitions(
                        allGraphs, program, globalValues, activeVertices);
                partitionGathered.put(pid, gathered);
            }

            // === APPLY ===
            Map<Integer, Set<Long>> partitionChanged = new HashMap<>();
            for (int pid : partitionIds) {
                Map<Long, V> values = partitionValues.get(pid);
                Set<Long> activeVertices = partitionActiveVertices.get(pid);
                Map<Long, M> gathered = partitionGathered.get(pid);
                Set<Long> changed = new HashSet<>();

                for (long vid : activeVertices) {
                    M msg = gathered.get(vid);
                    V currentVal = values.get(vid);
                    V newVal = program.apply(vid, currentVal, msg);
                    values.put(vid, newVal);

                    if (!Objects.equals(newVal, currentVal)) {
                        changed.add(vid);
                    }
                }
                partitionChanged.put(pid, changed);
            }

            // Rebuild global values after apply
            globalValues.clear();
            for (Map<Long, V> pv : partitionValues.values()) {
                globalValues.putAll(pv);
            }

            // === SCATTER ===
            Map<Integer, Set<Long>> nextActive = new HashMap<>();
            for (int pid : partitionIds) {
                nextActive.put(pid, new HashSet<>());
            }

            for (int pid : partitionIds) {
                Set<Long> changed = partitionChanged.get(pid);
                Map<Long, V> values = partitionValues.get(pid);

                for (long vid : changed) {
                    V newValue = values.get(vid);
                    // Build scatter triplets across ALL partitions
                    List<EdgeTriplet<V, E>> scatterTriplets =
                            buildTripletsAcrossPartitions(allGraphs, vid,
                                    program.scatterNeighbors(), globalValues);

                    for (EdgeTriplet<V, E> triplet : scatterTriplets) {
                        Set<Long> activated = program.scatter(triplet, newValue);
                        for (long activatedVid : activated) {
                            int targetPid = findPartitionForVertex(
                                    activatedVid, partitionAllVertexIds);
                            if (targetPid >= 0) {
                                nextActive.get(targetPid).add(activatedVid);
                            }
                        }
                    }
                }
            }

            for (int pid : partitionIds) {
                Set<Long> active = nextActive.get(pid);
                active.retainAll(partitionAllVertexIds.get(pid));
                partitionActiveVertices.put(pid, active);
            }
        }

        // Filter to only include vertices that exist in each partition
        for (int pid : partitionIds) {
            partitionValues.get(pid).keySet().retainAll(partitionAllVertexIds.get(pid));
        }

        return partitionValues;
    }

    /**
     * Execute a vertex program on a subset of active vertices across all partitions.
     * Used by distributed ICE for incremental computation.
     */
    public <V, E, M> Map<Integer, Map<Long, V>> executeOnSubgraph(
            ByteArray snapshotId,
            VertexProgram<V, E, M> program,
            Map<Long, V> currentValues,
            Map<Integer, Set<Long>> partitionActiveIds,
            int maxIterations) {

        List<Integer> partitionIds = cluster.localPartitions();

        List<GraphView> allGraphs = new ArrayList<>();
        Map<Integer, Map<Long, V>> partitionValues = new HashMap<>();
        Map<Integer, Set<Long>> partitionActiveVertices = new HashMap<>();
        Map<Integer, Set<Long>> partitionAllVertexIds = new HashMap<>();

        for (int pid : partitionIds) {
            GraphView graph = cluster.storeForPartition(pid).retrieve(snapshotId);
            allGraphs.add(graph);

            Set<Long> allVertexIds = collectVertexIds(graph);
            partitionAllVertexIds.put(pid, allVertexIds);

            Map<Long, V> values = new HashMap<>();
            for (long vid : allVertexIds) {
                if (currentValues.containsKey(vid)) {
                    values.put(vid, currentValues.get(vid));
                }
            }
            partitionValues.put(pid, values);

            Set<Long> active = partitionActiveIds.getOrDefault(pid, Set.of());
            partitionActiveVertices.put(pid, new HashSet<>(active));
        }

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            boolean anyActive = partitionIds.stream()
                    .anyMatch(pid -> !partitionActiveVertices.get(pid).isEmpty());
            if (!anyActive) {
                break;
            }

            Map<Long, V> globalValues = new HashMap<>();
            for (Map<Long, V> pv : partitionValues.values()) {
                globalValues.putAll(pv);
            }

            // Gather + Sum across all partitions
            Map<Integer, Map<Long, M>> partitionGathered = new HashMap<>();
            for (int pid : partitionIds) {
                Set<Long> activeVertices = partitionActiveVertices.get(pid);
                Map<Long, M> gathered = gatherAndSumAcrossPartitions(
                        allGraphs, program, globalValues, activeVertices);
                partitionGathered.put(pid, gathered);
            }

            // Apply
            Map<Integer, Set<Long>> partitionChanged = new HashMap<>();
            for (int pid : partitionIds) {
                Map<Long, V> values = partitionValues.get(pid);
                Set<Long> activeVertices = partitionActiveVertices.get(pid);
                Map<Long, M> gathered = partitionGathered.get(pid);
                Set<Long> changed = new HashSet<>();

                for (long vid : activeVertices) {
                    M msg = gathered.get(vid);
                    V currentVal = values.get(vid);
                    V newVal = program.apply(vid, currentVal, msg);
                    values.put(vid, newVal);

                    if (!Objects.equals(newVal, currentVal)) {
                        changed.add(vid);
                    }
                }
                partitionChanged.put(pid, changed);
            }

            globalValues.clear();
            for (Map<Long, V> pv : partitionValues.values()) {
                globalValues.putAll(pv);
            }

            // Scatter across all partitions
            Map<Integer, Set<Long>> nextActive = new HashMap<>();
            for (int pid : partitionIds) {
                nextActive.put(pid, new HashSet<>());
            }

            for (int pid : partitionIds) {
                Set<Long> changed = partitionChanged.get(pid);
                Map<Long, V> values = partitionValues.get(pid);

                for (long vid : changed) {
                    V newValue = values.get(vid);
                    List<EdgeTriplet<V, E>> scatterTriplets =
                            buildTripletsAcrossPartitions(allGraphs, vid,
                                    program.scatterNeighbors(), globalValues);

                    for (EdgeTriplet<V, E> triplet : scatterTriplets) {
                        Set<Long> activated = program.scatter(triplet, newValue);
                        for (long activatedVid : activated) {
                            int targetPid = findPartitionForVertex(
                                    activatedVid, partitionAllVertexIds);
                            if (targetPid >= 0) {
                                nextActive.get(targetPid).add(activatedVid);
                            }
                        }
                    }
                }
            }

            for (int pid : partitionIds) {
                Set<Long> active = nextActive.get(pid);
                active.retainAll(partitionAllVertexIds.get(pid));
                partitionActiveVertices.put(pid, active);
            }
        }

        for (int pid : partitionIds) {
            partitionValues.get(pid).keySet().retainAll(partitionAllVertexIds.get(pid));
        }

        return partitionValues;
    }

    /**
     * Gather and sum across ALL partition graphs for the given active vertices.
     * This handles the case where IN-edges for a vertex are stored in a different
     * partition than the vertex itself.
     */
    private <V, E, M> Map<Long, M> gatherAndSumAcrossPartitions(
            List<GraphView> allGraphs,
            VertexProgram<V, E, M> program,
            Map<Long, V> values,
            Set<Long> activeVertices) {

        Map<Long, M> gathered = new HashMap<>();

        for (long vid : activeVertices) {
            List<EdgeTriplet<V, E>> triplets =
                    buildTripletsAcrossPartitions(allGraphs, vid, program.gatherNeighbors(), values);

            M sum = null;
            for (EdgeTriplet<V, E> triplet : triplets) {
                M msg = program.gather(triplet);
                if (msg != null) {
                    sum = (sum == null) ? msg : program.sum(sum, msg);
                }
            }
            if (sum != null) {
                gathered.put(vid, sum);
            }
        }

        return gathered;
    }

    /**
     * Build edge triplets for a vertex by scanning ALL partition graphs.
     * Deduplicates edges to avoid double-counting.
     */
    @SuppressWarnings("unchecked")
    private <V, E> List<EdgeTriplet<V, E>> buildTripletsAcrossPartitions(
            List<GraphView> allGraphs, long vertexId,
            EdgeDirection direction, Map<Long, V> values) {

        List<EdgeTriplet<V, E>> triplets = new ArrayList<>();
        // Track seen edge keys to avoid duplicates
        Set<String> seenEdges = new HashSet<>();

        for (GraphView graph : allGraphs) {
            if (direction == EdgeDirection.OUT || direction == EdgeDirection.BOTH) {
                Iterator<EdgeData> outEdges = graph.outEdges(vertexId);
                while (outEdges.hasNext()) {
                    EdgeData ed = outEdges.next();
                    String edgeKey = ed.edgeKey().srcId() + ":" + ed.edgeKey().dstId()
                            + ":" + ed.edgeKey().discriminator();
                    if (seenEdges.add(edgeKey)) {
                        long srcId = ed.edgeKey().srcId();
                        long dstId = ed.edgeKey().dstId();
                        V srcVal = values.get(srcId);
                        V dstVal = values.get(dstId);
                        E edgeVal = (E) ed.properties();
                        triplets.add(new EdgeTriplet<>(srcId, srcVal, dstId, dstVal, edgeVal));
                    }
                }
            }

            if (direction == EdgeDirection.IN || direction == EdgeDirection.BOTH) {
                Iterator<EdgeData> inEdges = graph.inEdges(vertexId);
                while (inEdges.hasNext()) {
                    EdgeData ed = inEdges.next();
                    String edgeKey = ed.edgeKey().srcId() + ":" + ed.edgeKey().dstId()
                            + ":" + ed.edgeKey().discriminator();
                    if (seenEdges.add(edgeKey)) {
                        long srcId = ed.edgeKey().srcId();
                        long dstId = ed.edgeKey().dstId();
                        V srcVal = values.get(srcId);
                        V dstVal = values.get(dstId);
                        E edgeVal = (E) ed.properties();
                        triplets.add(new EdgeTriplet<>(srcId, srcVal, dstId, dstVal, edgeVal));
                    }
                }
            }
        }

        return triplets;
    }

    private int findPartitionForVertex(long vertexId, Map<Integer, Set<Long>> partitionAllVertexIds) {
        for (Map.Entry<Integer, Set<Long>> entry : partitionAllVertexIds.entrySet()) {
            if (entry.getValue().contains(vertexId)) {
                return entry.getKey();
            }
        }
        return -1;
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
