package org.tegra.cluster.compute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Routes messages between partitions during distributed GAS execution.
 * Uses in-memory concurrent queues for in-process operation.
 *
 * @param <M> message type
 */
public final class MessageRouter<M> {

    // partition ID -> queue of (vertexId, message) pairs
    private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<MessageEntry<M>>> queues;
    private final AtomicBoolean hasActive;

    public MessageRouter() {
        this.queues = new ConcurrentHashMap<>();
        this.hasActive = new AtomicBoolean(false);
    }

    /**
     * Sends a message to a vertex in the target partition.
     *
     * @param targetPartition the partition that owns the target vertex
     * @param vertexId        the target vertex ID
     * @param message         the message to send
     */
    public void sendMessage(int targetPartition, long vertexId, M message) {
        queues.computeIfAbsent(targetPartition, k -> new ConcurrentLinkedQueue<>())
                .add(new MessageEntry<>(vertexId, message));
        hasActive.set(true);
    }

    /**
     * Receives and drains all messages for the given partition.
     * Messages are aggregated by vertex ID. Calling this method clears
     * the queue for that partition.
     *
     * @param partitionId the partition to receive messages for
     * @return map of vertex ID to list of messages
     */
    public Map<Long, List<M>> receiveMessages(int partitionId) {
        ConcurrentLinkedQueue<MessageEntry<M>> queue = queues.get(partitionId);
        Map<Long, List<M>> result = new HashMap<>();
        if (queue != null) {
            MessageEntry<M> entry;
            while ((entry = queue.poll()) != null) {
                result.computeIfAbsent(entry.vertexId(), k -> new ArrayList<>())
                        .add(entry.message());
            }
        }
        return result;
    }

    /**
     * Barrier synchronization point. In the in-process implementation,
     * this is a no-op since all operations complete synchronously.
     * In a distributed setting, this would coordinate across nodes.
     */
    public void barrier() {
        // In-process: no-op; all operations are synchronous
    }

    /**
     * Returns true if there are any pending messages in any partition queue.
     */
    public boolean hasActiveVertices() {
        for (ConcurrentLinkedQueue<MessageEntry<M>> queue : queues.values()) {
            if (!queue.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Clears all message queues and resets active state.
     */
    public void clear() {
        queues.clear();
        hasActive.set(false);
    }

    /**
     * Internal message entry pairing a vertex ID with a message.
     */
    private record MessageEntry<M>(long vertexId, M message) {
    }
}
