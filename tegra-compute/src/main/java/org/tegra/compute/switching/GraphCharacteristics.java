package org.tegra.compute.switching;

/**
 * Static characteristics of a graph, used as features for the switching classifier.
 *
 * @param avgDegree             average degree of vertices
 * @param avgDiameter           average diameter of the graph
 * @param clusteringCoefficient clustering coefficient
 */
public record GraphCharacteristics(
        double avgDegree,
        double avgDiameter,
        double clusteringCoefficient
) {}
