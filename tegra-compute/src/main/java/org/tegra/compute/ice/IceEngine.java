package org.tegra.compute.ice;

import org.tegra.api.GraphDelta;
import org.tegra.api.GraphSnapshot;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.gas.VertexProgram;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Incremental Computation by Entity expansion (ICE) engine.
 * <p>
 * Given a new snapshot, a previous snapshot, and previously computed results,
 * ICE computes only the affected subgraph (changed vertices expanded by one hop)
 * and merges with the prior results for unchanged vertices. A {@link SwitchOracle}
 * may trigger a full recomputation if the affected region is too large.
 */
public final class IceEngine {

    private final GasEngine gasEngine;
    private final DiffEngine diffEngine;
    private final NeighborhoodExpander expander;
    private final SwitchOracle switchOracle;

    public IceEngine(GasEngine gasEngine, DiffEngine diffEngine, NeighborhoodExpander expander) {
        this(gasEngine, diffEngine, expander, SwitchOracle.defaultOracle());
    }

    public IceEngine(GasEngine gasEngine, DiffEngine diffEngine,
                     NeighborhoodExpander expander, SwitchOracle switchOracle) {
        this.gasEngine = gasEngine;
        this.diffEngine = diffEngine;
        this.expander = expander;
        this.switchOracle = switchOracle;
    }

    /**
     * Execute incrementally: given a new snapshot and previous results,
     * recompute only the affected subgraph.
     *
     * @param newSnapshot      the current graph snapshot
     * @param program          vertex program to execute
     * @param previousSnapshot the prior snapshot that produced {@code previousResults}
     * @param previousResults  results from the previous execution
     * @param <V>              vertex value type
     * @param <E>              edge value type
     * @param <M>              message type
     * @return complete results (merged incremental + unchanged)
     */
    public <V, E, M> Map<Long, V> executeIncremental(
            GraphSnapshot<V, E> newSnapshot,
            VertexProgram<V, E, M> program,
            GraphSnapshot<V, E> previousSnapshot,
            Map<Long, V> previousResults) {

        // 1. Diff new vs previous snapshot
        GraphDelta<V, E> delta = diffEngine.diff(previousSnapshot, newSnapshot);

        // 2. Find affected vertices (vertex changes + edge-endpoint changes)
        Set<Long> changedVertices = delta.affectedVertexIds();

        // If no changes, return previous results
        if (changedVertices.isEmpty()) {
            return new HashMap<>(previousResults);
        }

        // 3. Expand to 1-hop neighborhood
        Set<Long> affectedVertices = expander.expandOneHop(newSnapshot, changedVertices);

        // 4. Check switch oracle — if affected region is too large, do full recompute
        if (switchOracle.shouldSwitch(0, affectedVertices.size(),
                newSnapshot.vertexCount(), program)) {
            return gasEngine.execute(newSnapshot, program);
        }

        // 5. Run GAS only on affected subgraph
        Map<Long, V> incrementalResults = gasEngine.execute(newSnapshot, program, affectedVertices);

        // 6. Merge: start with previous results, overlay incremental
        Map<Long, V> mergedResults = new HashMap<>(previousResults);

        // Remove vertices that no longer exist in the new snapshot
        mergedResults.keySet().removeIf(vid -> newSnapshot.vertex(vid).isEmpty());

        // Overlay updated values for affected vertices
        for (long vid : affectedVertices) {
            if (incrementalResults.containsKey(vid)) {
                mergedResults.put(vid, incrementalResults.get(vid));
            }
        }

        // Add any newly added vertices not in previousResults
        for (var entry : incrementalResults.entrySet()) {
            mergedResults.putIfAbsent(entry.getKey(), entry.getValue());
        }

        return mergedResults;
    }
}
