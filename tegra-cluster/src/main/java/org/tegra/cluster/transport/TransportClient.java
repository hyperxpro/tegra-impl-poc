package org.tegra.cluster.transport;

import org.tegra.cluster.NodeDescriptor;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for an async RPC client.
 * Sends requests to remote cluster nodes and returns responses asynchronously.
 */
public interface TransportClient {

    /**
     * Sends a request to the specified target node.
     *
     * @param target  the target node descriptor
     * @param method  the RPC method name
     * @param payload the serialized request payload
     * @return a future that completes with the serialized response
     */
    CompletableFuture<byte[]> send(NodeDescriptor target, String method, byte[] payload);
}
