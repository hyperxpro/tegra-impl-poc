package org.tegra.compute.switching;

/**
 * A single training record for the switching classifier.
 *
 * @param metrics      iteration metrics at the decision point
 * @param graphChars   graph characteristics
 * @param shouldSwitch label: true if switching to full recomputation was faster
 */
public record TrainingRecord(
        IterationMetrics metrics,
        GraphCharacteristics graphChars,
        boolean shouldSwitch
) {}
