package org.tegra.benchmark.dataset;

import org.tegra.serde.EdgeData;
import org.tegra.serde.EdgeKey;
import org.tegra.serde.VertexData;
import org.tegra.store.partition.PartitionStore;
import org.tegra.store.partition.WorkingVersion;
import org.tegra.store.version.ByteArray;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Synthetic power-law graph generator using the R-MAT (Recursive MATrix) algorithm.
 * Produces graphs with a power-law degree distribution characteristic of real-world networks.
 * <p>
 * The algorithm recursively partitions a 2^scale x 2^scale adjacency matrix into
 * four quadrants. At each recursion level, a quadrant is chosen with probabilities
 * (a, b, c, d), producing edges that concentrate in the upper-left quadrant and
 * thus yield a skewed degree distribution.
 */
public final class RmatGraphGenerator implements DatasetLoader {

    private final int scale;
    private final int edgeFactor;
    private final double a;
    private final double b;
    private final double c;
    private final double d;
    private final long seed;

    /**
     * Creates an R-MAT generator with the given parameters.
     *
     * @param scale      log2 of the number of vertices (vertices = 2^scale)
     * @param edgeFactor edges per vertex (total edges = vertices * edgeFactor)
     * @param a          probability of upper-left quadrant (default 0.57)
     * @param b          probability of upper-right quadrant (default 0.19)
     * @param c          probability of lower-left quadrant (default 0.19)
     * @param d          probability of lower-right quadrant (default 0.05)
     * @param seed       random seed for reproducibility
     */
    public RmatGraphGenerator(int scale, int edgeFactor, double a, double b, double c, double d, long seed) {
        if (scale < 1 || scale > 30) {
            throw new IllegalArgumentException("scale must be between 1 and 30, got: " + scale);
        }
        double sum = a + b + c + d;
        if (Math.abs(sum - 1.0) > 1e-6) {
            throw new IllegalArgumentException("a + b + c + d must equal 1.0, got: " + sum);
        }
        this.scale = scale;
        this.edgeFactor = edgeFactor;
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
        this.seed = seed;
    }

    /**
     * Creates an R-MAT generator with default probabilities (0.57, 0.19, 0.19, 0.05).
     */
    public RmatGraphGenerator(int scale, int edgeFactor) {
        this(scale, edgeFactor, 0.57, 0.19, 0.19, 0.05, 42L);
    }

    /**
     * Creates an R-MAT generator with default probabilities and a specified seed.
     */
    public RmatGraphGenerator(int scale, int edgeFactor, long seed) {
        this(scale, edgeFactor, 0.57, 0.19, 0.19, 0.05, seed);
    }

    @Override
    public GraphLoadResult load(PartitionStore store, ByteArray versionId) {
        long startTime = System.nanoTime();
        long numVertices = 1L << scale;
        long targetEdges = numVertices * edgeFactor;

        // Create initial version and branch
        store.createInitialVersion(versionId);
        WorkingVersion wv = store.branch(versionId);

        // Add all vertices
        for (long v = 0; v < numVertices; v++) {
            wv.putVertex(v, new VertexData(v, Map.of()));
        }

        // Generate edges using R-MAT algorithm
        Random rng = new Random(seed);
        Set<Long> edgeSet = new HashSet<>(); // track unique edges to avoid duplicates
        long edgeCount = 0;

        for (long e = 0; e < targetEdges; e++) {
            long[] edge = generateEdge(rng, numVertices);
            long srcId = edge[0];
            long dstId = edge[1];

            // Skip self-loops
            if (srcId == dstId) {
                continue;
            }

            // Skip duplicates using Cantor pairing
            long pairKey = srcId * numVertices + dstId;
            if (!edgeSet.add(pairKey)) {
                continue;
            }

            wv.putEdge(srcId, dstId, (short) 0,
                    new EdgeData(new EdgeKey(srcId, dstId, (short) 0), Map.of()));
            edgeCount++;
        }

        // Commit
        store.evict(versionId); // remove the initial empty version
        // Re-create with actual data
        org.tegra.store.version.VersionEntry entry = new org.tegra.store.version.VersionEntry(
                wv.vertexRoot(), wv.edgeRoot(), System.currentTimeMillis(), null);
        store.versionMap().put(versionId, entry);

        long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
        return new GraphLoadResult(versionId, numVertices, edgeCount, elapsedMs);
    }

    /**
     * Generates a single edge using the R-MAT recursive quadrant selection algorithm.
     * <p>
     * Starting with a numVertices x numVertices adjacency matrix, at each of the
     * {@code scale} recursion levels, the matrix is divided into 4 quadrants.
     * A quadrant is chosen with probabilities (a, b, c, d) and the coordinate
     * range is halved accordingly. After all levels, the final (row, col) is the edge.
     */
    private long[] generateEdge(Random rng, long numVertices) {
        long srcId = 0;
        long dstId = 0;

        for (int level = scale - 1; level >= 0; level--) {
            double r = rng.nextDouble();
            long halfSize = 1L << level;

            if (r < a) {
                // Upper-left quadrant: no offset
            } else if (r < a + b) {
                // Upper-right quadrant: offset dstId
                dstId += halfSize;
            } else if (r < a + b + c) {
                // Lower-left quadrant: offset srcId
                srcId += halfSize;
            } else {
                // Lower-right quadrant: offset both
                srcId += halfSize;
                dstId += halfSize;
            }
        }

        return new long[]{srcId, dstId};
    }

    /**
     * Returns the scale parameter.
     */
    public int scale() {
        return scale;
    }

    /**
     * Returns the edge factor.
     */
    public int edgeFactor() {
        return edgeFactor;
    }
}
