package org.tegra.compute.gas;

/**
 * A triplet combining source vertex, destination vertex, and edge data
 * for use in GAS vertex programs.
 *
 * @param srcId     source vertex ID
 * @param srcValue  current value of the source vertex
 * @param dstId     destination vertex ID
 * @param dstValue  current value of the destination vertex
 * @param edgeValue value associated with the edge
 * @param <V>       vertex value type
 * @param <E>       edge value type
 */
public record EdgeTriplet<V, E>(
        long srcId, V srcValue,
        long dstId, V dstValue,
        E edgeValue
) {}
