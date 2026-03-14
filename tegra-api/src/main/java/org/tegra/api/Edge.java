package org.tegra.api;

/**
 * An edge with associated properties.
 *
 * @param src        the source vertex ID
 * @param dst        the destination vertex ID
 * @param properties the edge properties
 * @param <E>        the property type
 */
public record Edge<E>(long src, long dst, E properties) {
}
