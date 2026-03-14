package org.tegra.pds.common;

/**
 * Result of a path-copying operation on a persistent data structure node.
 *
 * @param <N> the node type
 * @param newNode    the new or modified node
 * @param sizeChange the change in number of entries (+1 for insert, -1 for remove, 0 for update)
 */
public record PathCopyResult<N>(N newNode, int sizeChange) {
}
