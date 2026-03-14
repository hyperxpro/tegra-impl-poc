package org.tegra.compute.switching;

import org.tegra.compute.ice.SwitchOracle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Random forest classifier implementing SwitchOracle.
 * Predicts whether to switch from incremental to full re-execution
 * at iteration boundaries, based on 10 features (7 iteration metrics + 3 graph characteristics).
 * <p>
 * Self-contained implementation with no external ML dependencies.
 */
public final class SwitchingClassifier implements SwitchOracle {

    private static final int NUM_FEATURES = 10;
    private static final int DEFAULT_NUM_TREES = 50;
    private static final int DEFAULT_MAX_DEPTH = 10;
    private static final int DEFAULT_MIN_SAMPLES_SPLIT = 2;

    private final RandomForestModel model;

    private SwitchingClassifier(RandomForestModel model) {
        this.model = model;
    }

    /**
     * Trains a switching classifier from labeled training data.
     *
     * @param data list of training records
     * @return a trained classifier
     */
    public static SwitchingClassifier train(List<TrainingRecord> data) {
        return train(data, DEFAULT_NUM_TREES, DEFAULT_MAX_DEPTH, DEFAULT_MIN_SAMPLES_SPLIT, new Random(42));
    }

    /**
     * Trains with custom hyperparameters.
     */
    public static SwitchingClassifier train(
            List<TrainingRecord> data, int numTrees, int maxDepth, int minSamplesSplit, Random rng) {
        if (data.isEmpty()) {
            throw new IllegalArgumentException("Training data must not be empty");
        }

        double[][] features = new double[data.size()][];
        boolean[] labels = new boolean[data.size()];

        for (int i = 0; i < data.size(); i++) {
            features[i] = extractFeatures(data.get(i).metrics(), data.get(i).graphChars());
            labels[i] = data.get(i).shouldSwitch();
        }

        List<DecisionTree> trees = new ArrayList<>();
        for (int t = 0; t < numTrees; t++) {
            // Bootstrap sample
            int n = features.length;
            double[][] bootFeatures = new double[n][];
            boolean[] bootLabels = new boolean[n];
            for (int i = 0; i < n; i++) {
                int idx = rng.nextInt(n);
                bootFeatures[i] = features[idx];
                bootLabels[i] = labels[idx];
            }

            DecisionTree tree = DecisionTree.build(bootFeatures, bootLabels, maxDepth, minSamplesSplit, rng);
            trees.add(tree);
        }

        return new SwitchingClassifier(new RandomForestModel(trees));
    }

    @Override
    public boolean shouldSwitch(int affectedCount, long totalVertexCount) {
        // Simple delegation using default metrics when only counts are available
        IterationMetrics metrics = new IterationMetrics(
                affectedCount,
                0.0,
                1,
                0.0,
                0.0,
                0L,
                0L
        );
        GraphCharacteristics graph = new GraphCharacteristics(0.0, 0.0, 0.0);
        return shouldSwitch(metrics, graph);
    }

    /**
     * Predicts whether to switch using full metrics and graph characteristics.
     *
     * @param metrics iteration metrics at the decision point
     * @param graph   graph characteristics
     * @return true if switching to full recomputation is predicted to be faster
     */
    public boolean shouldSwitch(IterationMetrics metrics, GraphCharacteristics graph) {
        double[] features = extractFeatures(metrics, graph);
        return model.predict(features);
    }

    /**
     * Extracts the 10-dimensional feature vector from metrics and graph characteristics.
     */
    static double[] extractFeatures(IterationMetrics metrics, GraphCharacteristics graph) {
        return new double[]{
                metrics.activeVertexCount(),
                metrics.avgDegreeOfActiveVertices(),
                metrics.activePartitionCount(),
                metrics.msgsGeneratedPerVertex(),
                metrics.msgsReceivedPerVertex(),
                metrics.networkBytesTransferred(),
                metrics.iterationTimeMs(),
                graph.avgDegree(),
                graph.avgDiameter(),
                graph.clusteringCoefficient()
        };
    }

    // ---- Inner classes for the random forest ----

    /**
     * Ensemble of decision trees using majority vote.
     */
    record RandomForestModel(List<DecisionTree> trees) {

        boolean predict(double[] features) {
            int votes = 0;
            for (DecisionTree tree : trees) {
                if (tree.predict(features)) {
                    votes++;
                }
            }
            return votes > trees.size() / 2;
        }
    }

    /**
     * Binary classification decision tree.
     * Each internal node splits on a single feature at a threshold.
     * Leaf nodes hold a class prediction.
     */
    sealed interface DecisionTree {

        boolean predict(double[] features);

        /**
         * Builds a decision tree from the given training data.
         */
        static DecisionTree build(double[][] features, boolean[] labels,
                                  int maxDepth, int minSamplesSplit, Random rng) {
            return buildRecursive(features, labels, 0, maxDepth, minSamplesSplit, rng);
        }

        private static DecisionTree buildRecursive(
                double[][] features, boolean[] labels,
                int depth, int maxDepth, int minSamplesSplit, Random rng) {

            int n = features.length;

            // Count classes
            int trueCount = 0;
            for (boolean label : labels) {
                if (label) trueCount++;
            }
            int falseCount = n - trueCount;

            // Terminal conditions: pure node, max depth, or too few samples
            if (trueCount == 0 || falseCount == 0 || depth >= maxDepth || n < minSamplesSplit) {
                return new Leaf(trueCount >= falseCount);
            }

            // Select random feature subset (sqrt(numFeatures) features)
            int numFeatures = features[0].length;
            int subsetSize = Math.max(1, (int) Math.sqrt(numFeatures));
            int[] featureSubset = new int[subsetSize];
            boolean[] selected = new boolean[numFeatures];
            for (int i = 0; i < subsetSize; i++) {
                int f;
                do {
                    f = rng.nextInt(numFeatures);
                } while (selected[f]);
                selected[f] = true;
                featureSubset[i] = f;
            }

            // Find best split
            double bestGini = Double.MAX_VALUE;
            int bestFeature = -1;
            double bestThreshold = 0.0;

            for (int fi : featureSubset) {
                // Collect unique values and sort
                double[] vals = new double[n];
                for (int i = 0; i < n; i++) {
                    vals[i] = features[i][fi];
                }
                Arrays.sort(vals);

                // Try midpoints between consecutive distinct values
                for (int i = 0; i < n - 1; i++) {
                    if (vals[i] == vals[i + 1]) continue;
                    double threshold = (vals[i] + vals[i + 1]) / 2.0;

                    // Compute Gini impurity for this split
                    int leftTrue = 0, leftFalse = 0, rightTrue = 0, rightFalse = 0;
                    for (int j = 0; j < n; j++) {
                        if (features[j][fi] <= threshold) {
                            if (labels[j]) leftTrue++;
                            else leftFalse++;
                        } else {
                            if (labels[j]) rightTrue++;
                            else rightFalse++;
                        }
                    }

                    int leftTotal = leftTrue + leftFalse;
                    int rightTotal = rightTrue + rightFalse;
                    if (leftTotal == 0 || rightTotal == 0) continue;

                    double giniLeft = 1.0
                            - Math.pow((double) leftTrue / leftTotal, 2)
                            - Math.pow((double) leftFalse / leftTotal, 2);
                    double giniRight = 1.0
                            - Math.pow((double) rightTrue / rightTotal, 2)
                            - Math.pow((double) rightFalse / rightTotal, 2);

                    double weightedGini = ((double) leftTotal / n) * giniLeft
                            + ((double) rightTotal / n) * giniRight;

                    if (weightedGini < bestGini) {
                        bestGini = weightedGini;
                        bestFeature = fi;
                        bestThreshold = threshold;
                    }
                }
            }

            // If no valid split found, return leaf
            if (bestFeature < 0) {
                return new Leaf(trueCount >= falseCount);
            }

            // Partition data
            List<Integer> leftIndices = new ArrayList<>();
            List<Integer> rightIndices = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                if (features[i][bestFeature] <= bestThreshold) {
                    leftIndices.add(i);
                } else {
                    rightIndices.add(i);
                }
            }

            double[][] leftFeatures = new double[leftIndices.size()][];
            boolean[] leftLabels = new boolean[leftIndices.size()];
            for (int i = 0; i < leftIndices.size(); i++) {
                leftFeatures[i] = features[leftIndices.get(i)];
                leftLabels[i] = labels[leftIndices.get(i)];
            }

            double[][] rightFeatures = new double[rightIndices.size()][];
            boolean[] rightLabels = new boolean[rightIndices.size()];
            for (int i = 0; i < rightIndices.size(); i++) {
                rightFeatures[i] = features[rightIndices.get(i)];
                rightLabels[i] = labels[rightIndices.get(i)];
            }

            DecisionTree leftChild = buildRecursive(leftFeatures, leftLabels, depth + 1, maxDepth, minSamplesSplit, rng);
            DecisionTree rightChild = buildRecursive(rightFeatures, rightLabels, depth + 1, maxDepth, minSamplesSplit, rng);

            return new Split(bestFeature, bestThreshold, leftChild, rightChild);
        }

        /**
         * Internal node: splits on feature at threshold.
         */
        record Split(int featureIndex, double threshold, DecisionTree left, DecisionTree right)
                implements DecisionTree {
            @Override
            public boolean predict(double[] features) {
                if (features[featureIndex] <= threshold) {
                    return left.predict(features);
                } else {
                    return right.predict(features);
                }
            }
        }

        /**
         * Leaf node: holds a class prediction.
         */
        record Leaf(boolean prediction) implements DecisionTree {
            @Override
            public boolean predict(double[] features) {
                return prediction;
            }
        }
    }
}
