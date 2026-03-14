package org.tegra.compute.gas;

/**
 * Specifies which edge directions to consider during GAS phases.
 */
public enum EdgeDirection {
    /** Only incoming edges. */
    IN,
    /** Only outgoing edges. */
    OUT,
    /** Both incoming and outgoing edges. */
    BOTH
}
