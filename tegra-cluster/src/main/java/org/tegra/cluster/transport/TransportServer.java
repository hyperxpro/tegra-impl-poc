package org.tegra.cluster.transport;

/**
 * Interface for an NIO-based RPC server.
 * Handles incoming requests by dispatching to registered method handlers.
 */
public interface TransportServer {

    /**
     * Starts the server on the given port.
     *
     * @param port the port to listen on
     */
    void start(int port);

    /**
     * Stops the server and releases resources.
     */
    void stop();

    /**
     * Registers a handler for the given RPC method.
     *
     * @param method  the method name
     * @param handler the request handler
     */
    void registerHandler(String method, RequestHandler handler);
}
