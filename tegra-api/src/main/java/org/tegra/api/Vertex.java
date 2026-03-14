package org.tegra.api;

/**
 * A vertex with associated properties.
 *
 * @param id         the vertex identifier
 * @param properties the vertex properties
 * @param <V>        the property type
 */
public record Vertex<V>(long id, V properties) {
}
