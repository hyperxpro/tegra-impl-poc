package org.tegra.compute.gas;

import org.tegra.api.Edge;
import org.tegra.api.EdgeDirection;
import org.tegra.api.GraphSnapshot;
import org.tegra.api.Vertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Executes a {@link VertexProgram} on a {@link GraphSnapshot} using the
 * Gather-Apply-Scatter paradigm.
 * <p>
 * The engine iterates supersteps until every vertex has converged or the
 * program's {@link VertexProgram#maxIterations() maxIterations} bound is reached.
 */
public final class GasEngine {

    /**
     * Execute a vertex program on the given graph snapshot.
     *
     * @param snapshot the immutable graph to compute over
     * @param program  the vertex program defining gather/apply/scatter logic
     * @param <V>      vertex value type
     * @param <E>      edge value type
     * @param <M>      message type
     * @return map of vertex ID to final computed value
     */
    public <V, E, M> Map<Long, V> execute(
            GraphSnapshot<V, E> snapshot,
            VertexProgram<V, E, M> program) {

        return execute(snapshot, program, null);
    }

    /**
     * Execute a vertex program, restricting computation to a subset of vertices.
     * If {@code activeSubset} is {@code null}, all vertices are initially active.
     *
     * @param snapshot     the immutable graph to compute over
     * @param program      the vertex program
     * @param activeSubset initial active vertex set, or {@code null} for all
     * @param <V>          vertex value type
     * @param <E>          edge value type
     * @param <M>          message type
     * @return map of vertex ID to final computed value
     */
    public <V, E, M> Map<Long, V> execute(
            GraphSnapshot<V, E> snapshot,
            VertexProgram<V, E, M> program,
            Set<Long> activeSubset) {

        var context = new GasContext<V, E, M>(program);

        // Initialize vertex values from snapshot properties
        Map<Long, V> values = new HashMap<>();
        snapshot.vertices().forEach(v -> values.put(v.id(), v.properties()));

        // Initialize active set
        Set<Long> initialActive;
        if (activeSubset != null) {
            initialActive = new HashSet<>(activeSubset);
        } else {
            initialActive = new HashSet<>(values.keySet());
        }
        context.setActiveVertices(initialActive);

        // Superstep loop
        while (context.hasActiveVertices() && !context.hasReachedMaxIterations()) {
            context.clearMessages();

            // --- GATHER ---
            for (long vid : context.activeVertices()) {
                V vertexValue = values.get(vid);
                if (vertexValue == null) continue;

                gatherFromNeighbors(snapshot, program, values, context, vid, vertexValue);
            }

            // --- APPLY ---
            Map<Long, V> oldValues = new HashMap<>();
            for (long vid : context.activeVertices()) {
                V currentValue = values.get(vid);
                if (currentValue == null) continue;

                oldValues.put(vid, currentValue);
                M gathered = context.getMessage(vid);
                V newValue = program.apply(currentValue, gathered);
                values.put(vid, newValue);
            }

            // --- SCATTER ---
            Set<Long> nextActive = new HashSet<>();
            for (long vid : context.activeVertices()) {
                V updatedValue = values.get(vid);
                V oldValue = oldValues.get(vid);
                if (updatedValue == null) continue;

                if (!program.hasConverged(oldValue, updatedValue)) {
                    scatterToNeighbors(snapshot, program, nextActive, vid, updatedValue, oldValue);
                }
            }

            context.setActiveVertices(nextActive);
            context.advanceIteration();
        }

        return values;
    }

    private <V, E, M> void gatherFromNeighbors(
            GraphSnapshot<V, E> snapshot,
            VertexProgram<V, E, M> program,
            Map<Long, V> values,
            GasContext<V, E, M> context,
            long vid,
            V vertexValue) {

        EdgeDirection dir = program.gatherDirection();

        if (dir == EdgeDirection.IN || dir == EdgeDirection.BOTH) {
            snapshot.inEdges(vid).forEach(edge -> {
                V neighborValue = values.get(edge.src());
                if (neighborValue != null) {
                    M msg = program.gather(vertexValue, edge.properties(), neighborValue);
                    context.sendMessage(vid, msg);
                }
            });
        }

        if (dir == EdgeDirection.OUT || dir == EdgeDirection.BOTH) {
            snapshot.outEdges(vid).forEach(edge -> {
                V neighborValue = values.get(edge.dst());
                if (neighborValue != null) {
                    M msg = program.gather(vertexValue, edge.properties(), neighborValue);
                    context.sendMessage(vid, msg);
                }
            });
        }
    }

    private <V, E, M> void scatterToNeighbors(
            GraphSnapshot<V, E> snapshot,
            VertexProgram<V, E, M> program,
            Set<Long> nextActive,
            long vid,
            V updatedValue,
            V oldValue) {

        EdgeDirection dir = program.scatterDirection();

        if (dir == EdgeDirection.OUT || dir == EdgeDirection.BOTH) {
            snapshot.outEdges(vid).forEach(edge -> {
                if (program.scatter(updatedValue, oldValue, edge.properties())) {
                    nextActive.add(edge.dst());
                }
            });
        }

        if (dir == EdgeDirection.IN || dir == EdgeDirection.BOTH) {
            snapshot.inEdges(vid).forEach(edge -> {
                if (program.scatter(updatedValue, oldValue, edge.properties())) {
                    nextActive.add(edge.src());
                }
            });
        }
    }
}
