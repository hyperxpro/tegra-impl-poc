package org.tegra.compute.ice;

/**
 * Default switch oracle using a simple threshold heuristic.
 * If the fraction of affected vertices exceeds the threshold,
 * switches to full re-execution.
 */
public final class HeuristicSwitchOracle implements SwitchOracle {

    private final double threshold;

    /**
     * Creates a heuristic oracle with the given threshold.
     *
     * @param threshold fraction (0.0 to 1.0) above which to switch to full computation
     */
    public HeuristicSwitchOracle(double threshold) {
        if (threshold < 0.0 || threshold > 1.0) {
            throw new IllegalArgumentException("Threshold must be between 0.0 and 1.0, got: " + threshold);
        }
        this.threshold = threshold;
    }

    /**
     * Creates a heuristic oracle with the default threshold of 0.5.
     */
    public HeuristicSwitchOracle() {
        this(0.5);
    }

    @Override
    public boolean shouldSwitch(int affectedCount, long totalVertexCount) {
        if (totalVertexCount == 0) {
            return false;
        }
        return (double) affectedCount / totalVertexCount > threshold;
    }

    /**
     * Returns the configured threshold.
     */
    public double threshold() {
        return threshold;
    }
}
