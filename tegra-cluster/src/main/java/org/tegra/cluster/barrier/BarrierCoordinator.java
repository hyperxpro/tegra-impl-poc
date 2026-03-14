package org.tegra.cluster.barrier;

import org.tegra.store.version.ByteArray;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Coordinates two-phase distributed snapshot commits across all partition nodes.
 * <p>
 * Phase 1 (prepare): All partitions prepare for commit in parallel.
 * Phase 2 (commit): After all partitions are prepared, all commit in parallel.
 * <p>
 * This ensures all partitions commit at the same logical time, producing
 * a consistent distributed snapshot.
 */
public final class BarrierCoordinator {

    private final List<PartitionNode> partitions;

    public BarrierCoordinator(List<PartitionNode> partitions) {
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("partitions must not be null or empty");
        }
        this.partitions = List.copyOf(partitions);
    }

    /**
     * Coordinates a distributed snapshot commit across all partitions.
     *
     * @param versionId the version ID for the new snapshot
     * @return a future that completes with the committed version ID
     */
    public CompletableFuture<ByteArray> coordinateCommit(ByteArray versionId) {
        // Phase 1: Prepare all partitions in parallel
        List<CompletableFuture<Void>> prepareFutures = new ArrayList<>();
        for (PartitionNode node : partitions) {
            prepareFutures.add(CompletableFuture.runAsync(() -> node.prepareCommit(versionId)));
        }

        CompletableFuture<Void> allPrepared = CompletableFuture.allOf(
                prepareFutures.toArray(CompletableFuture[]::new));

        // Phase 2: After all prepared, commit all partitions in parallel
        return allPrepared.thenCompose(ignored -> {
            List<CompletableFuture<Void>> commitFutures = new ArrayList<>();
            for (PartitionNode node : partitions) {
                commitFutures.add(CompletableFuture.runAsync(() -> node.commit(versionId)));
            }
            return CompletableFuture.allOf(commitFutures.toArray(CompletableFuture[]::new));
        }).thenApply(ignored -> versionId);
    }
}
