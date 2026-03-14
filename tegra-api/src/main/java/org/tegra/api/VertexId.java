package org.tegra.api;

/**
 * Simple vertex identifier wrapping a long value.
 */
public record VertexId(long id) implements Comparable<VertexId> {

    @Override
    public int compareTo(VertexId other) {
        return Long.compare(id, other.id);
    }
}
