package org.tegra.store.log;

import org.tegra.serde.EdgeData;
import org.tegra.serde.PropertyValue;
import org.tegra.serde.VertexData;

/**
 * Sealed interface for graph mutations.
 * Each mutation type is a record representing a single atomic change to the graph.
 */
public sealed interface GraphMutation
        permits GraphMutation.AddVertex,
                GraphMutation.RemoveVertex,
                GraphMutation.AddEdge,
                GraphMutation.RemoveEdge,
                GraphMutation.UpdateVertexProperty,
                GraphMutation.UpdateEdgeProperty {

    /**
     * Adds a vertex to the graph.
     */
    record AddVertex(VertexData vertexData) implements GraphMutation {
    }

    /**
     * Removes a vertex from the graph.
     */
    record RemoveVertex(long vertexId) implements GraphMutation {
    }

    /**
     * Adds an edge to the graph.
     */
    record AddEdge(EdgeData edgeData) implements GraphMutation {
    }

    /**
     * Removes an edge from the graph.
     */
    record RemoveEdge(long srcId, long dstId, short discriminator) implements GraphMutation {
    }

    /**
     * Updates a property on a vertex.
     */
    record UpdateVertexProperty(long vertexId, String propertyKey, PropertyValue value) implements GraphMutation {
    }

    /**
     * Updates a property on an edge.
     */
    record UpdateEdgeProperty(long srcId, long dstId, short discriminator,
                              String propertyKey, PropertyValue value) implements GraphMutation {
    }
}
