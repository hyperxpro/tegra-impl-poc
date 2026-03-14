package org.tegra.api;

/**
 * Direction of edge traversal, following the GAS (Gather-Apply-Scatter) model.
 */
public enum EdgeDirection {
    /** Incoming edges only. */
    IN,
    /** Outgoing edges only. */
    OUT,
    /** Both incoming and outgoing edges. */
    BOTH
}
