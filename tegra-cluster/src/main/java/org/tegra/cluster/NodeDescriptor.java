package org.tegra.cluster;

import java.util.List;

/**
 * Descriptor for a cluster node.
 *
 * @param host               hostname or IP address
 * @param port               RPC port
 * @param nodeId             unique node identifier
 * @param assignedPartitions list of partition IDs assigned to this node
 */
public record NodeDescriptor(
        String host,
        int port,
        int nodeId,
        List<Integer> assignedPartitions
) {
    public NodeDescriptor {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host must not be null or blank");
        }
        if (assignedPartitions == null) {
            throw new IllegalArgumentException("assignedPartitions must not be null");
        }
        assignedPartitions = List.copyOf(assignedPartitions);
    }
}
