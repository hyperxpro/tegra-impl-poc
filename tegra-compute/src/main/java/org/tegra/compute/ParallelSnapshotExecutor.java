package org.tegra.compute;

import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.gas.VertexProgram;
import org.tegra.compute.ice.IceEngine;
import org.tegra.store.GraphView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;

/**
 * Executes vertex programs across multiple graph snapshots in parallel
 * using virtual threads. The first snapshot uses full GAS execution;
 * subsequent snapshots use ICE incremental computation from the
 * nearest prior result.
 */
public final class ParallelSnapshotExecutor {

    private final GasEngine gasEngine;
    private final IceEngine iceEngine;

    public ParallelSnapshotExecutor(GasEngine gasEngine, IceEngine iceEngine) {
        this.gasEngine = gasEngine;
        this.iceEngine = iceEngine;
    }

    /**
     * Executes a vertex program on multiple graph snapshots in parallel.
     * The first snapshot gets full GAS execution. Subsequent snapshots
     * use ICE incremental computation referencing the previous snapshot's result.
     *
     * @param graphs         ordered list of graph snapshots to compute on
     * @param program        the vertex program
     * @param initialValues  initial vertex values
     * @param maxIterations  maximum supersteps per execution
     * @param <V>            vertex value type
     * @param <E>            edge value type
     * @param <M>            message type
     * @return map from snapshot index to computed vertex values
     */
    public <V, E, M> Map<Integer, Map<Long, V>> executeParallel(
            List<GraphView> graphs,
            VertexProgram<V, E, M> program,
            Map<Long, V> initialValues,
            int maxIterations) {

        if (graphs.isEmpty()) {
            return Map.of();
        }

        Map<Integer, Map<Long, V>> results = new ConcurrentHashMap<>();

        // First snapshot: full GAS execution (must complete before ICE can start)
        Map<Long, V> firstResult = gasEngine.execute(graphs.get(0), program, initialValues, maxIterations);
        results.put(0, firstResult);

        if (graphs.size() == 1) {
            return results;
        }

        // Subsequent snapshots: ICE incremental, each depending on prior result.
        // Use virtual threads for parallelism where dependency allows.
        // In the simplest correct implementation, each snapshot depends on the previous one.
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            // We process snapshots sequentially in dependency order,
            // but each ICE computation itself runs on a virtual thread.
            Map<Long, V> previousResult = firstResult;
            GraphView previousGraph = graphs.get(0);

            for (int i = 1; i < graphs.size(); i++) {
                final int index = i;
                final Map<Long, V> prevRes = previousResult;
                final GraphView prevGraph = previousGraph;
                final GraphView currentGraph = graphs.get(i);

                var subtask = scope.fork(() ->
                        iceEngine.incPregel(currentGraph, prevRes, prevGraph, program, maxIterations)
                );

                scope.join();
                scope.throwIfFailed();

                Map<Long, V> result = subtask.get();
                results.put(index, result);

                previousResult = result;
                previousGraph = currentGraph;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Parallel snapshot execution interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Parallel snapshot execution failed", e);
        }

        return results;
    }
}
