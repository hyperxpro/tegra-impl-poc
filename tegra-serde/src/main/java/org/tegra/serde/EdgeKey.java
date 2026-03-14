package org.tegra.serde;

/**
 * Composite key for an edge in the graph.
 *
 * @param srcId         source vertex ID
 * @param dstId         destination vertex ID
 * @param discriminator discriminator for multi-edges between the same pair
 */
public record EdgeKey(long srcId, long dstId, short discriminator) {
}
