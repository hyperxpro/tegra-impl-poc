package org.tegra.compute.ice;

import org.tegra.api.Delta;
import org.tegra.compute.gas.GasEngine;
import org.tegra.compute.gas.VertexProgram;
import org.tegra.store.GraphView;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ICE (Incremental Computation by Entity expansion) engine.
 * Implements the core incremental computation algorithm from the TEGRA paper (Listing 1).
 * <p>
 * Instead of running GAS on the entire graph, ICE:
 * 1. Diffs the current graph against the previous graph to find affected vertices.
 * 2. Expands the affected set by 1-hop (for the gather phase).
 * 3. Checks a switch oracle to decide if full recomputation is cheaper.
 * 4. Runs GAS only on the affected subgraph.
 * 5. Copies previous results for unaffected vertices.
 */
public final class IceEngine {

    private final GasEngine gasEngine;
    private final SwitchOracle switchOracle;
    private final DiffEngine diffEngine;
    private final NeighborhoodExpander expander;

    public IceEngine(GasEngine gasEngine, SwitchOracle switchOracle) {
        this.gasEngine = gasEngine;
        this.switchOracle = switchOracle;
        this.diffEngine = new DiffEngine();
        this.expander = new NeighborhoodExpander();
    }

    /**
     * Incremental Pregel implementation.
     * Computes vertex values on the current graph by reusing previous results
     * and only recomputing affected vertices.
     *
     * @param currentGraph    the current graph snapshot
     * @param previousResults previous computation results (vertex ID to value)
     * @param previousGraph   the previous graph snapshot (for diffing)
     * @param program         the vertex program
     * @param maxIterations   maximum number of supersteps
     * @param <V>             vertex value type
     * @param <E>             edge value type
     * @param <M>             message type
     * @return map of vertex ID to computed value
     */
    public <V, E, M> Map<Long, V> incPregel(
            GraphView currentGraph,
            Map<Long, V> previousResults,
            GraphView previousGraph,
            VertexProgram<V, E, M> program,
            int maxIterations) {

        // Step 1: Compute diff between current and previous graph
        Delta delta = diffEngine.computeDiff(currentGraph, previousGraph);

        // If no changes, return previous results (adjusted for any removed vertices)
        if (delta.isEmpty()) {
            return new HashMap<>(previousResults);
        }

        // Step 2: Identify affected vertices from the delta
        Set<Long> affectedVertices = delta.affectedVertices();

        // Step 3: Expand affected by 1-hop based on gather direction
        // The expansion ensures the gather phase has correct neighbor data
        var subgraphView = expander.expand(affectedVertices, currentGraph, program.gatherNeighbors());
        Set<Long> expandedActive = new HashSet<>(subgraphView.activeVertexIds());
        expandedActive.addAll(subgraphView.boundaryVertexIds());

        // Step 4: Check switch oracle - if too many affected, do full computation
        long totalVertexCount = currentGraph.vertexCount();
        if (switchOracle.shouldSwitch(expandedActive.size(), totalVertexCount)) {
            // Fall back to full GAS computation using previous results as starting point
            return gasEngine.execute(currentGraph, program, new HashMap<>(previousResults), maxIterations);
        }

        // Step 5: Run GAS on affected subgraph only
        // Start with previous results as initial values
        Map<Long, V> currentValues = new HashMap<>(previousResults);

        // Remove entries for deleted vertices
        for (long vid : delta.removedVertices()) {
            currentValues.remove(vid);
        }

        // Run GAS only on the affected subgraph
        Map<Long, V> result = gasEngine.executeOnSubgraph(
                currentGraph, program, currentValues,
                subgraphView.activeVertexIds(), maxIterations);

        return result;
    }
}
