package org.tegra.benchmark.dataset;

import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Loads a graph from a SNAP-format edge list TSV file.
 * <p>
 * File format: each line is "srcId\tdstId" (tab-separated).
 * Lines starting with '#' are treated as comments and skipped.
 * Vertices are created implicitly from the edges.
 */
public final class SnapDatasetLoader implements DatasetLoader {

    private final Path filePath;

    /**
     * Creates a loader for the given SNAP edge list file.
     *
     * @param filePath path to the TSV edge list file
     */
    public SnapDatasetLoader(Path filePath) {
        if (filePath == null) {
            throw new IllegalArgumentException("filePath must not be null");
        }
        this.filePath = filePath;
    }

    @Override
    public GraphLoadResult load(PartitionStore store, ByteArray versionId) {
        long startTime = System.nanoTime();

        store.createInitialVersion(versionId);
        WorkingVersion wv = store.branch(versionId);

        Set<Long> vertexIds = new HashSet<>();
        long edgeCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // Skip comments and empty lines
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                String[] parts = line.split("\\s+");
                if (parts.length < 2) {
                    continue;
                }

                long srcId = Long.parseLong(parts[0]);
                long dstId = Long.parseLong(parts[1]);

                // Add vertices if not yet seen
                if (vertexIds.add(srcId)) {
                    wv.putVertex(srcId, new VertexData(srcId, Map.of()));
                }
                if (vertexIds.add(dstId)) {
                    wv.putVertex(dstId, new VertexData(dstId, Map.of()));
                }

                // Add edge
                wv.putEdge(srcId, dstId, (short) 0,
                        new EdgeData(new EdgeKey(srcId, dstId, (short) 0), Map.of()));
                edgeCount++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read SNAP file: " + filePath, e);
        }

        // Commit
        store.evict(versionId);
        org.tegra.store.version.VersionEntry entry = new org.tegra.store.version.VersionEntry(
                wv.vertexRoot(), wv.edgeRoot(), System.currentTimeMillis(), null);
        store.versionMap().put(versionId, entry);

        long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
        return new GraphLoadResult(versionId, vertexIds.size(), edgeCount, elapsedMs);
    }
}
