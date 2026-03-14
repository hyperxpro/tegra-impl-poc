package org.tegra.algorithms;

import org.tegra.compute.gas.EdgeDirection;
import org.tegra.compute.gas.EdgeTriplet;
import org.tegra.compute.gas.VertexProgram;

import java.util.Arrays;
import java.util.Set;

/**
 * Generalized Belief Propagation on factor graphs.
 * Each vertex holds a belief vector (probability distribution over states).
 * Messages are belief vectors that get element-wise multiplied and normalized.
 */
public final class BeliefPropagation implements VertexProgram<double[], Object, double[]> {

    private final int numStates;
    private final double[][] transitionMatrix;
    private final double convergenceThreshold;

    /**
     * @param numStates        number of discrete states
     * @param transitionMatrix state transition probability matrix (numStates x numStates)
     */
    public BeliefPropagation(int numStates, double[][] transitionMatrix) {
        this(numStates, transitionMatrix, 1e-6);
    }

    public BeliefPropagation(int numStates, double[][] transitionMatrix, double convergenceThreshold) {
        this.numStates = numStates;
        this.transitionMatrix = transitionMatrix;
        this.convergenceThreshold = convergenceThreshold;
    }

    @Override
    public double[] gather(EdgeTriplet<double[], Object> context) {
        // Collect the neighbor's belief vector, transformed through the transition matrix.
        // For an edge (src -> dst), when gathering for dst, the neighbor is src.
        // For an edge (src -> dst), when gathering for src (via inEdges), neighbor is dst...
        // but with BOTH direction we get both. We transform the srcValue through the transition matrix.
        double[] neighborBelief = context.srcValue();
        if (neighborBelief == null) {
            return null;
        }
        // Apply transition matrix: message[i] = sum_j(T[i][j] * belief[j])
        double[] message = new double[numStates];
        for (int i = 0; i < numStates; i++) {
            for (int j = 0; j < numStates; j++) {
                message[i] += transitionMatrix[i][j] * neighborBelief[j];
            }
        }
        return message;
    }

    @Override
    public double[] sum(double[] a, double[] b) {
        // Element-wise multiply and normalize
        double[] result = new double[numStates];
        double total = 0.0;
        for (int i = 0; i < numStates; i++) {
            result[i] = a[i] * b[i];
            total += result[i];
        }
        // Normalize to prevent underflow
        if (total > 0) {
            for (int i = 0; i < numStates; i++) {
                result[i] /= total;
            }
        }
        return result;
    }

    @Override
    public double[] apply(long vertexId, double[] currentValue, double[] gathered) {
        if (currentValue == null) {
            currentValue = uniformBelief();
        }
        if (gathered == null) {
            return currentValue;
        }
        // Multiply prior (current belief) with gathered messages, then normalize
        double[] result = new double[numStates];
        double total = 0.0;
        for (int i = 0; i < numStates; i++) {
            result[i] = currentValue[i] * gathered[i];
            total += result[i];
        }
        if (total > 0) {
            for (int i = 0; i < numStates; i++) {
                result[i] /= total;
            }
        }
        return result;
    }

    @Override
    public Set<Long> scatter(EdgeTriplet<double[], Object> context, double[] newValue) {
        // Activate neighbors if beliefs changed significantly
        double[] oldValue = context.srcValue();
        if (oldValue == null || beliefDifference(newValue, oldValue) > convergenceThreshold) {
            return Set.of(context.srcId(), context.dstId());
        }
        return Set.of();
    }

    @Override
    public EdgeDirection gatherNeighbors() {
        return EdgeDirection.BOTH;
    }

    @Override
    public EdgeDirection scatterNeighbors() {
        return EdgeDirection.BOTH;
    }

    private double[] uniformBelief() {
        double[] belief = new double[numStates];
        Arrays.fill(belief, 1.0 / numStates);
        return belief;
    }

    private double beliefDifference(double[] a, double[] b) {
        if (a == null || b == null) return Double.MAX_VALUE;
        double maxDiff = 0.0;
        for (int i = 0; i < Math.min(a.length, b.length); i++) {
            maxDiff = Math.max(maxDiff, Math.abs(a[i] - b[i]));
        }
        return maxDiff;
    }
}
