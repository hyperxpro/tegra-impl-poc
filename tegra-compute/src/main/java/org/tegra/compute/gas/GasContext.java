package org.tegra.compute.gas;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mutable per-superstep state for the GAS engine.
 * Holds the message inbox and active vertex set.
 *
 * @param <M> message type
 */
public final class GasContext<M> {

    private final Map<Long, List<M>> messages = new HashMap<>();
    private final Set<Long> activeVertices = new HashSet<>();

    /**
     * Sends a message to a target vertex.
     *
     * @param targetVertexId the destination vertex
     * @param message        the message to send
     */
    public void sendMessage(long targetVertexId, M message) {
        if (message != null) {
            messages.computeIfAbsent(targetVertexId, k -> new ArrayList<>()).add(message);
        }
    }

    /**
     * Returns the list of messages for the given vertex, or an empty list if none.
     */
    public List<M> getMessages(long vertexId) {
        return messages.getOrDefault(vertexId, List.of());
    }

    /**
     * Returns all vertex IDs that have pending messages.
     */
    public Set<Long> verticesWithMessages() {
        return messages.keySet();
    }

    /**
     * Activates a vertex for the next superstep.
     */
    public void activateVertex(long vertexId) {
        activeVertices.add(vertexId);
    }

    /**
     * Returns true if the vertex is active.
     */
    public boolean isActive(long vertexId) {
        return activeVertices.contains(vertexId);
    }

    /**
     * Returns the set of active vertices.
     */
    public Set<Long> activeVertexSet() {
        return activeVertices;
    }

    /**
     * Clears all messages and active vertices for the next superstep.
     */
    public void reset() {
        messages.clear();
        activeVertices.clear();
    }

    /**
     * Returns true if there are no active vertices.
     */
    public boolean hasNoActiveVertices() {
        return activeVertices.isEmpty();
    }
}
