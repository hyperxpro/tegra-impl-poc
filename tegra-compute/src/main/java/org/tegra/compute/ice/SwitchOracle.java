package org.tegra.compute.ice;

import org.tegra.compute.gas.VertexProgram;

/**
 * Heuristic oracle that decides when an incremental computation should be
 * abandoned in favour of a full recomputation.
 * <p>
 * When the "active frontier" grows large relative to the total graph, incremental
 * processing loses its advantage and a full GAS pass is cheaper.
 */
public interface SwitchOracle {

    /**
     * Determine whether to switch from incremental to full recomputation.
     *
     * @param currentIteration  current superstep number
     * @param activeVertexCount number of currently active vertices
     * @param totalVertexCount  total vertices in the graph
     * @param program           the running vertex program
     * @return {@code true} if a full recomputation should be triggered
     */
    boolean shouldSwitch(int currentIteration, int activeVertexCount,
                         long totalVertexCount, VertexProgram<?, ?, ?> program);

    /**
     * Returns the default heuristic oracle.
     */
    static SwitchOracle defaultOracle() {
        return new HeuristicSwitchOracle();
    }
}
