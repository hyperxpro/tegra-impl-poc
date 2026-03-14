package org.tegra.compute.ice;

import org.tegra.compute.gas.VertexProgram;

/**
 * Default {@link SwitchOracle} implementation that switches to full recomputation
 * when the active vertex ratio exceeds a configurable threshold.
 */
public final class HeuristicSwitchOracle implements SwitchOracle {

    private final double threshold;

    /**
     * Create an oracle with the default threshold of 0.5 (switch when more than
     * half the graph is active).
     */
    public HeuristicSwitchOracle() {
        this(0.5);
    }

    /**
     * Create an oracle with a custom threshold.
     *
     * @param threshold ratio (0.0–1.0) of active/total vertices at which to switch
     */
    public HeuristicSwitchOracle(double threshold) {
        if (threshold <= 0.0 || threshold > 1.0) {
            throw new IllegalArgumentException("Threshold must be in (0, 1]: " + threshold);
        }
        this.threshold = threshold;
    }

    @Override
    public boolean shouldSwitch(int currentIteration, int activeVertexCount,
                                long totalVertexCount, VertexProgram<?, ?, ?> program) {
        if (totalVertexCount == 0) {
            return false;
        }
        double ratio = (double) activeVertexCount / totalVertexCount;
        return ratio > threshold;
    }
}
