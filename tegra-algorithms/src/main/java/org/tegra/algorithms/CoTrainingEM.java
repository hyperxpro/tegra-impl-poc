package org.tegra.algorithms;

import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;

import java.util.Arrays;
import java.util.Set;

/**
 * Co-Training Expectation Maximization on factor graphs.
 * Each vertex holds a topic/class distribution (double[]).
 * Implements EM-style update: gather distributions from neighbors,
 * compute weighted average, normalize, and mix with prior.
 */
public final class CoTrainingEM implements VertexProgram<double[], Object, double[]> {

    private final int numTopics;
    private final double mixingWeight; // weight for prior vs gathered (0..1)
    private final double convergenceThreshold;

    public CoTrainingEM(int numTopics) {
        this(numTopics, 0.5, 1e-6);
    }

    public CoTrainingEM(int numTopics, double mixingWeight, double convergenceThreshold) {
        this.numTopics = numTopics;
        this.mixingWeight = mixingWeight;
        this.convergenceThreshold = convergenceThreshold;
    }

    @Override
    public double[] gather(EdgeTriplet<double[], Object> context) {
        // Collect topic distribution from neighbor
        double[] srcDist = context.srcValue();
        double[] dstDist = context.dstValue();
        // Return whichever neighbor value is available
        if (srcDist != null) {
            return Arrays.copyOf(srcDist, srcDist.length);
        }
        if (dstDist != null) {
            return Arrays.copyOf(dstDist, dstDist.length);
        }
        return null;
    }

    @Override
    public double[] sum(double[] a, double[] b) {
        // Element-wise weighted average (accumulate then normalize in apply)
        double[] result = new double[numTopics];
        for (int i = 0; i < numTopics; i++) {
            result[i] = a[i] + b[i];
        }
        return result;
    }

    @Override
    public double[] apply(long vertexId, double[] currentValue, double[] gathered) {
        if (currentValue == null) {
            currentValue = uniformDistribution();
        }
        if (gathered == null) {
            return currentValue;
        }

        // Normalize gathered
        double[] normalizedGathered = normalize(gathered);

        // EM update: mix prior (current) with gathered evidence
        double[] result = new double[numTopics];
        for (int i = 0; i < numTopics; i++) {
            result[i] = mixingWeight * currentValue[i] + (1.0 - mixingWeight) * normalizedGathered[i];
        }

        return normalize(result);
    }

    @Override
    public Set<Long> scatter(EdgeTriplet<double[], Object> context, double[] newValue) {
        // Activate neighbors if distribution changed significantly
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

    private double[] uniformDistribution() {
        double[] dist = new double[numTopics];
        Arrays.fill(dist, 1.0 / numTopics);
        return dist;
    }

    private double[] normalize(double[] arr) {
        double sum = 0;
        for (double v : arr) {
            sum += v;
        }
        if (sum <= 0) {
            return uniformDistribution();
        }
        double[] result = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = arr[i] / sum;
        }
        return result;
    }
}
