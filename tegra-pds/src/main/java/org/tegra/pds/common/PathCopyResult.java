package org.tegra.pds.common;

/**
 * Result of a path-copy operation in a persistent tree.
 *
 * @param newRoot     the new root node after the path copy
 * @param copiedNodes the number of nodes that were copied
 * @param <N>         the node type
 */
public record PathCopyResult<N>(N newRoot, int copiedNodes) {
}
