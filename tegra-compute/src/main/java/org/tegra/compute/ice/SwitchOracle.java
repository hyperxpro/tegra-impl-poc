package org.tegra.compute.ice;

/**
 * Decides whether ICE should switch from incremental computation
 * to full re-execution. Used at iteration boundaries.
 */
public interface SwitchOracle {

    /**
     * Returns true if the engine should switch to full re-execution.
     *
     * @param affectedCount    number of affected vertices
     * @param totalVertexCount total number of vertices in the graph
     * @return true to switch to full computation, false to continue incremental
     */
    boolean shouldSwitch(int affectedCount, long totalVertexCount);
}
