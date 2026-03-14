package org.tegra.compute.gas;

import org.tegra.serde.EdgeData;
import org.tegra.store.GraphView;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Iterative GAS (Gather-Apply-Scatter) superstep executor.
 * Runs a VertexProgram on a GraphView until convergence or max iterations.
 * <p>
 * The engine does not modify the graph — it computes vertex values externally
 * in a {@code Map<Long, V>}.
 */
public final class GasEngine {

    /**
     * Execute a vertex program on a graph.
     *
     * @param graph          the graph to compute on
     * @param program        the vertex program
     * @param initialValues  initial vertex values (vertex ID to value)
     * @param maxIterations  maximum number of supersteps
     * @param <V>            vertex value type
     * @param <E>            edge value type
     * @param <M>            message type
     * @return map of vertex ID to final computed value
     */
    public <V, E, M> Map<Long, V> execute(
            GraphView graph,
            VertexProgram<V, E, M> program,
            Map<Long, V> initialValues,
            int maxIterations) {

        // Copy initial values
        Map<Long, V> values = new HashMap<>(initialValues);

        // Collect all vertex IDs from the graph
        Set<Long> allVertexIds = collectVertexIds(graph);

        // Initially, all vertices with values are active
        Set<Long> activeVertices = new HashSet<>(allVertexIds);

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            if (activeVertices.isEmpty()) {
                break;
            }

            // === GATHER + SUM ===
            Map<Long, M> gathered = gatherAndSum(graph, program, values, activeVertices);

            // === APPLY ===
            Map<Long, V> oldValues = new HashMap<>(values);
            Set<Long> changedVertices = new HashSet<>();

            for (long vid : activeVertices) {
                M msg = gathered.get(vid);
                V currentVal = values.get(vid);
                V newVal = program.apply(vid, currentVal, msg);
                values.put(vid, newVal);

                // Track changes
                if (!java.util.Objects.equals(newVal, currentVal)) {
                    changedVertices.add(vid);
                }
            }

            // === SCATTER ===
            Set<Long> nextActive = new HashSet<>();
            for (long vid : changedVertices) {
                V newValue = values.get(vid);
                List<EdgeTriplet<V, E>> scatterTriplets =
                        buildTriplets(graph, vid, program.scatterNeighbors(), values);
                for (EdgeTriplet<V, E> triplet : scatterTriplets) {
                    Set<Long> activated = program.scatter(triplet, newValue);
                    nextActive.addAll(activated);
                }
            }

            // Only keep vertices that actually exist in the graph
            nextActive.retainAll(allVertexIds);
            activeVertices = nextActive;
        }

        // Only return values for vertices that exist in the graph
        values.keySet().retainAll(allVertexIds);
        return values;
    }

    /**
     * Execute a vertex program on a graph, restricted to a subset of vertices.
     * Used by ICE for incremental computation on affected subgraphs.
     *
     * @param graph            the full graph
     * @param program          the vertex program
     * @param currentValues    current vertex values (all vertices, not just active)
     * @param activeVertexIds  the set of vertices to compute on
     * @param maxIterations    maximum number of supersteps
     * @param <V>              vertex value type
     * @param <E>              edge value type
     * @param <M>              message type
     * @return map of vertex ID to computed value (includes unmodified vertices)
     */
    public <V, E, M> Map<Long, V> executeOnSubgraph(
            GraphView graph,
            VertexProgram<V, E, M> program,
            Map<Long, V> currentValues,
            Set<Long> activeVertexIds,
            int maxIterations) {

        Map<Long, V> values = new HashMap<>(currentValues);
        Set<Long> activeVertices = new HashSet<>(activeVertexIds);
        Set<Long> allVertexIds = collectVertexIds(graph);

        for (int iteration = 0; iteration < maxIterations; iteration++) {
            if (activeVertices.isEmpty()) {
                break;
            }

            // === GATHER + SUM ===
            Map<Long, M> gathered = gatherAndSum(graph, program, values, activeVertices);

            // === APPLY ===
            Set<Long> changedVertices = new HashSet<>();
            for (long vid : activeVertices) {
                M msg = gathered.get(vid);
                V currentVal = values.get(vid);
                V newVal = program.apply(vid, currentVal, msg);
                values.put(vid, newVal);

                if (!java.util.Objects.equals(newVal, currentVal)) {
                    changedVertices.add(vid);
                }
            }

            // === SCATTER ===
            Set<Long> nextActive = new HashSet<>();
            for (long vid : changedVertices) {
                V newValue = values.get(vid);
                List<EdgeTriplet<V, E>> scatterTriplets =
                        buildTriplets(graph, vid, program.scatterNeighbors(), values);
                for (EdgeTriplet<V, E> triplet : scatterTriplets) {
                    Set<Long> activated = program.scatter(triplet, newValue);
                    nextActive.addAll(activated);
                }
            }

            nextActive.retainAll(allVertexIds);
            activeVertices = nextActive;
        }

        // Only return values for vertices that exist in the graph
        values.keySet().retainAll(allVertexIds);
        return values;
    }

    private Set<Long> collectVertexIds(GraphView graph) {
        Set<Long> ids = new HashSet<>();
        var it = graph.vertices();
        while (it.hasNext()) {
            ids.add(it.next().vertexId());
        }
        return ids;
    }

    private <V, E, M> Map<Long, M> gatherAndSum(
            GraphView graph,
            VertexProgram<V, E, M> program,
            Map<Long, V> values,
            Set<Long> activeVertices) {

        Map<Long, M> gathered = new HashMap<>();

        for (long vid : activeVertices) {
            List<EdgeTriplet<V, E>> triplets =
                    buildTriplets(graph, vid, program.gatherNeighbors(), values);

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

    @SuppressWarnings("unchecked")
    private <V, E> List<EdgeTriplet<V, E>> buildTriplets(
            GraphView graph, long vertexId,
            EdgeDirection direction, Map<Long, V> values) {

        List<EdgeTriplet<V, E>> triplets = new ArrayList<>();

        if (direction == EdgeDirection.OUT || direction == EdgeDirection.BOTH) {
            Iterator<EdgeData> outEdges = graph.outEdges(vertexId);
            while (outEdges.hasNext()) {
                EdgeData ed = outEdges.next();
                long srcId = ed.edgeKey().srcId();
                long dstId = ed.edgeKey().dstId();
                V srcVal = values.get(srcId);
                V dstVal = values.get(dstId);
                // Edge value is the EdgeData properties map — cast to E
                E edgeVal = (E) ed.properties();
                triplets.add(new EdgeTriplet<>(srcId, srcVal, dstId, dstVal, edgeVal));
            }
        }

        if (direction == EdgeDirection.IN || direction == EdgeDirection.BOTH) {
            Iterator<EdgeData> inEdges = graph.inEdges(vertexId);
            while (inEdges.hasNext()) {
                EdgeData ed = inEdges.next();
                long srcId = ed.edgeKey().srcId();
                long dstId = ed.edgeKey().dstId();
                V srcVal = values.get(srcId);
                V dstVal = values.get(dstId);
                E edgeVal = (E) ed.properties();
                triplets.add(new EdgeTriplet<>(srcId, srcVal, dstId, dstVal, edgeVal));
            }
        }

        return triplets;
    }
}
