package org.tegra.compute.switching;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the SwitchingClassifier (random forest implementation).
 */
class SwitchingClassifierTest {

    @Test
    void trainAndPredictSeparableData() {
        // Create linearly separable training data:
        // High active vertex count + high avg degree => should switch
        // Low active vertex count + low avg degree => should not switch
        List<TrainingRecord> data = new ArrayList<>();
        Random rng = new Random(42);

        for (int i = 0; i < 100; i++) {
            long activeCount = 100 + rng.nextInt(50);
            double avgDegree = 10.0 + rng.nextDouble() * 5;
            data.add(new TrainingRecord(
                    new IterationMetrics(activeCount, avgDegree, 4, 5.0, 5.0, 10000, 500),
                    new GraphCharacteristics(avgDegree, 5.0, 0.3),
                    true
            ));
        }

        for (int i = 0; i < 100; i++) {
            long activeCount = 5 + rng.nextInt(10);
            double avgDegree = 1.0 + rng.nextDouble();
            data.add(new TrainingRecord(
                    new IterationMetrics(activeCount, avgDegree, 1, 0.5, 0.5, 100, 10),
                    new GraphCharacteristics(avgDegree, 2.0, 0.1),
                    false
            ));
        }

        SwitchingClassifier classifier = SwitchingClassifier.train(data);

        // Test with clearly high metrics => should switch
        boolean highResult = classifier.shouldSwitch(
                new IterationMetrics(120, 12.0, 4, 5.0, 5.0, 10000, 500),
                new GraphCharacteristics(12.0, 5.0, 0.3)
        );
        assertThat(highResult).isTrue();

        // Test with clearly low metrics => should not switch
        boolean lowResult = classifier.shouldSwitch(
                new IterationMetrics(7, 1.5, 1, 0.5, 0.5, 100, 10),
                new GraphCharacteristics(1.5, 2.0, 0.1)
        );
        assertThat(lowResult).isFalse();
    }

    @Test
    void trainWithCustomHyperparameters() {
        List<TrainingRecord> data = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            data.add(new TrainingRecord(
                    new IterationMetrics(100, 10.0, 4, 5.0, 5.0, 10000, 500),
                    new GraphCharacteristics(10.0, 5.0, 0.3),
                    true
            ));
            data.add(new TrainingRecord(
                    new IterationMetrics(5, 1.0, 1, 0.5, 0.5, 100, 10),
                    new GraphCharacteristics(1.0, 2.0, 0.1),
                    false
            ));
        }

        SwitchingClassifier classifier = SwitchingClassifier.train(data, 10, 5, 3, new Random(123));
        assertThat(classifier).isNotNull();

        // Should still make reasonable predictions
        boolean result = classifier.shouldSwitch(
                new IterationMetrics(100, 10.0, 4, 5.0, 5.0, 10000, 500),
                new GraphCharacteristics(10.0, 5.0, 0.3)
        );
        assertThat(result).isTrue();
    }

    @Test
    void switchOracleInterfaceDelegation() {
        // Test the SwitchOracle interface method
        List<TrainingRecord> data = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            data.add(new TrainingRecord(
                    new IterationMetrics(100, 10.0, 4, 5.0, 5.0, 10000, 500),
                    new GraphCharacteristics(10.0, 5.0, 0.3),
                    true
            ));
            data.add(new TrainingRecord(
                    new IterationMetrics(5, 1.0, 1, 0.5, 0.5, 100, 10),
                    new GraphCharacteristics(1.0, 2.0, 0.1),
                    false
            ));
        }

        SwitchingClassifier classifier = SwitchingClassifier.train(data);

        // Test the simple shouldSwitch(int, long) interface
        // This uses default metrics with the counts
        boolean result = classifier.shouldSwitch(100, 200);
        // Just verify it doesn't throw — the result depends on the model
        assertThat(result).isIn(true, false);
    }

    @Test
    void emptyTrainingDataThrows() {
        assertThatThrownBy(() -> SwitchingClassifier.train(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void allSameLabelProducesUnanimousModel() {
        List<TrainingRecord> data = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            data.add(new TrainingRecord(
                    new IterationMetrics(50 + i, 5.0, 2, 2.0, 2.0, 5000, 200),
                    new GraphCharacteristics(5.0, 3.0, 0.2),
                    true
            ));
        }

        SwitchingClassifier classifier = SwitchingClassifier.train(data);

        // All training data says "switch", so model should predict "switch"
        boolean result = classifier.shouldSwitch(
                new IterationMetrics(60, 5.0, 2, 2.0, 2.0, 5000, 200),
                new GraphCharacteristics(5.0, 3.0, 0.2)
        );
        assertThat(result).isTrue();
    }
}
