package org.tegra.cluster.transport;

/**
 * Functional interface for handling RPC requests.
 */
@FunctionalInterface
public interface RequestHandler {

    /**
     * Handles an incoming request and returns a response.
     *
     * @param request the serialized request payload
     * @return the serialized response payload
     */
    byte[] handle(byte[] request);
}
