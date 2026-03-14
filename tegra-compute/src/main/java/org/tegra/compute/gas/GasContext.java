package org.tegra.compute.gas;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Mutable execution context tracking the state of a GAS computation across supersteps.
 *
 * @param <V> vertex value type
 * @param <E> edge value type
 * @param <M> message type
 */
public final class GasContext<V, E, M> {

    private final VertexProgram<V, E, M> program;
    private int currentIteration;
    private Set<Long> activeVertices;
    private Map<Long, M> messages;

    public GasContext(VertexProgram<V, E, M> program) {
        this.program = program;
        this.currentIteration = 0;
        this.activeVertices = new HashSet<>();
        this.messages = new HashMap<>();
    }

    public VertexProgram<V, E, M> program() {
        return program;
    }

    public int currentIteration() {
        return currentIteration;
    }

    public void advanceIteration() {
        currentIteration++;
    }

    public Set<Long> activeVertices() {
        return Collections.unmodifiableSet(activeVertices);
    }

    public void setActiveVertices(Set<Long> vertices) {
        this.activeVertices = new HashSet<>(vertices);
    }

    public Map<Long, M> messages() {
        return Collections.unmodifiableMap(messages);
    }

    public void clearMessages() {
        messages.clear();
    }

    /**
     * Accumulate a message for a vertex, combining via {@link VertexProgram#sum} if one already exists.
     */
    public void sendMessage(long vertexId, M message) {
        messages.merge(vertexId, message, program::sum);
    }

    public boolean hasMessage(long vertexId) {
        return messages.containsKey(vertexId);
    }

    public M getMessage(long vertexId) {
        return messages.getOrDefault(vertexId, program.identity());
    }

    public boolean hasReachedMaxIterations() {
        return currentIteration >= program.maxIterations();
    }

    public boolean hasActiveVertices() {
        return !activeVertices.isEmpty();
    }
}
