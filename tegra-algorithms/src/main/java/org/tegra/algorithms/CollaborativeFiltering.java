package org.tegra.algorithms;

import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;
import org.tegra.serde.PropertyValue;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Collaborative Filtering via ALS (Alternating Least Squares) matrix factorization.
 * Operates on a bipartite user-item graph where edge properties contain ratings.
 * Each vertex holds a factor vector (double[]).
 * <p>
 * The message type is double[] which encodes both the ATA matrix and ATb vector
 * in a flattened array: [ATA_flat (k*k) | ATb (k)].
 * <p>
 * Edge value type is Object because the GasEngine passes edge properties
 * (Map&lt;String, PropertyValue&gt;) as the edge value via unchecked cast.
 */
public final class CollaborativeFiltering implements VertexProgram<double[], Object, double[]> {

    private final int numFactors;
    private final double lambda;

    public CollaborativeFiltering() {
        this(10, 0.01);
    }

    public CollaborativeFiltering(int numFactors, double lambda) {
        this.numFactors = numFactors;
        this.lambda = lambda;
    }

    @Override
    public double[] gather(EdgeTriplet<double[], Object> context) {
        // Gather the neighbor's factor vector weighted by the edge rating.
        // For BOTH direction, we get triplets from both in/out edges.
        // When gathering for vertex V:
        //   - outEdge triplet: V is src, neighbor is dst -> use dstValue
        //   - inEdge triplet:  V is dst, neighbor is src -> use srcValue
        // We take whichever is non-null (both endpoints are valid neighbors).
        double[] neighborFactors = context.srcValue();
        if (neighborFactors == null) {
            neighborFactors = context.dstValue();
        }
        if (neighborFactors == null) {
            return null;
        }

        double rating = extractRating(context.edgeValue());

        int k = numFactors;
        double[] msg = new double[k * k + k];

        // ATA += factor * factor^T
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                msg[i * k + j] = neighborFactors[i] * neighborFactors[j];
            }
        }

        // ATb += factor * rating
        for (int i = 0; i < k; i++) {
            msg[k * k + i] = neighborFactors[i] * rating;
        }

        return msg;
    }

    @Override
    public double[] sum(double[] a, double[] b) {
        double[] result = new double[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = a[i] + b[i];
        }
        return result;
    }

    @Override
    public double[] apply(long vertexId, double[] currentValue, double[] gathered) {
        if (currentValue == null) {
            currentValue = new double[numFactors];
            Arrays.fill(currentValue, 0.1);
        }
        if (gathered == null) {
            return currentValue;
        }

        int k = numFactors;
        double[][] ata = new double[k][k];
        double[] atb = new double[k];

        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                ata[i][j] = gathered[i * k + j];
            }
            atb[i] = gathered[k * k + i];
        }

        // Add regularization: ATA += lambda * I
        for (int i = 0; i < k; i++) {
            ata[i][i] += lambda;
        }

        return solveLinearSystem(ata, atb);
    }

    @Override
    public Set<Long> scatter(EdgeTriplet<double[], Object> context, double[] newValue) {
        return Set.of(context.srcId(), context.dstId());
    }

    @Override
    public EdgeDirection gatherNeighbors() {
        return EdgeDirection.BOTH;
    }

    @Override
    public EdgeDirection scatterNeighbors() {
        return EdgeDirection.BOTH;
    }

    @SuppressWarnings("unchecked")
    private double extractRating(Object edgeValue) {
        if (edgeValue instanceof Map<?, ?> map) {
            Object ratingObj = map.get("rating");
            if (ratingObj instanceof PropertyValue.DoubleProperty dp) {
                return dp.value();
            }
        }
        return 1.0;
    }

    /**
     * Solves Ax = b using Gaussian elimination with partial pivoting.
     */
    private double[] solveLinearSystem(double[][] a, double[] b) {
        int n = b.length;
        double[][] aug = new double[n][n + 1];
        for (int i = 0; i < n; i++) {
            System.arraycopy(a[i], 0, aug[i], 0, n);
            aug[i][n] = b[i];
        }

        for (int col = 0; col < n; col++) {
            int maxRow = col;
            double maxVal = Math.abs(aug[col][col]);
            for (int row = col + 1; row < n; row++) {
                if (Math.abs(aug[row][col]) > maxVal) {
                    maxVal = Math.abs(aug[row][col]);
                    maxRow = row;
                }
            }
            double[] temp = aug[col];
            aug[col] = aug[maxRow];
            aug[maxRow] = temp;

            if (Math.abs(aug[col][col]) < 1e-12) {
                continue;
            }

            for (int row = col + 1; row < n; row++) {
                double factor = aug[row][col] / aug[col][col];
                for (int j = col; j <= n; j++) {
                    aug[row][j] -= factor * aug[col][j];
                }
            }
        }

        double[] x = new double[n];
        for (int i = n - 1; i >= 0; i--) {
            if (Math.abs(aug[i][i]) < 1e-12) {
                x[i] = 0;
                continue;
            }
            x[i] = aug[i][n];
            for (int j = i + 1; j < n; j++) {
                x[i] -= aug[i][j] * x[j];
            }
            x[i] /= aug[i][i];
        }

        return x;
    }
}
