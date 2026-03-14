package org.tegra.store;

import org.tegra.api.Edge;
import org.tegra.pds.hamt.PersistentHAMT;

import java.time.Instant;
import java.util.List;

/**
 * Immutable root pair capturing the persistent data structures for a single
 * graph snapshot version.
 *
 * @param vertexData vertex ID to vertex properties
 * @param outEdges   vertex ID to outgoing edge list
 * @param inEdges    vertex ID to incoming edge list
 * @param timestamp  when this version was created
 * @param <V>        vertex property type
 * @param <E>        edge property type
 */
public record VersionRoot<V, E>(
        PersistentHAMT<Long, V> vertexData,
        PersistentHAMT<Long, List<Edge<E>>> outEdges,
        PersistentHAMT<Long, List<Edge<E>>> inEdges,
        Instant timestamp
) {}
