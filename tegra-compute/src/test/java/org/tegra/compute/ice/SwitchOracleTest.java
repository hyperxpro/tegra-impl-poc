package org.tegra.compute.ice;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the HeuristicSwitchOracle.
 */
class SwitchOracleTest {

    @Test
    void belowThresholdDoesNotSwitch() {
        HeuristicSwitchOracle oracle = new HeuristicSwitchOracle(0.5);

        // 2 out of 10 = 0.2 < 0.5
        assertThat(oracle.shouldSwitch(2, 10)).isFalse();
    }

    @Test
    void aboveThresholdSwitches() {
        HeuristicSwitchOracle oracle = new HeuristicSwitchOracle(0.5);

        // 8 out of 10 = 0.8 > 0.5
        assertThat(oracle.shouldSwitch(8, 10)).isTrue();
    }

    @Test
    void atBoundaryDoesNotSwitch() {
        HeuristicSwitchOracle oracle = new HeuristicSwitchOracle(0.5);

        // 5 out of 10 = 0.5 exactly at threshold — NOT strictly greater
        assertThat(oracle.shouldSwitch(5, 10)).isFalse();
    }

    @Test
    void justAboveBoundarySwitches() {
        HeuristicSwitchOracle oracle = new HeuristicSwitchOracle(0.5);

        // 6 out of 10 = 0.6 > 0.5
        assertThat(oracle.shouldSwitch(6, 10)).isTrue();
    }

    @Test
    void zeroTotalVerticesDoesNotSwitch() {
        HeuristicSwitchOracle oracle = new HeuristicSwitchOracle(0.5);
        assertThat(oracle.shouldSwitch(0, 0)).isFalse();
    }

    @Test
    void allVerticesAffected() {
        HeuristicSwitchOracle oracle = new HeuristicSwitchOracle(0.5);

        // All 10 vertices affected = 1.0 > 0.5
        assertThat(oracle.shouldSwitch(10, 10)).isTrue();
    }

    @Test
    void defaultThresholdIs05() {
        HeuristicSwitchOracle oracle = new HeuristicSwitchOracle();
        assertThat(oracle.threshold()).isEqualTo(0.5);
    }

    @Test
    void customThreshold() {
        HeuristicSwitchOracle oracle = new HeuristicSwitchOracle(0.3);

        // 4 out of 10 = 0.4 > 0.3
        assertThat(oracle.shouldSwitch(4, 10)).isTrue();
        // 2 out of 10 = 0.2 < 0.3
        assertThat(oracle.shouldSwitch(2, 10)).isFalse();
    }

    @Test
    void invalidThresholdThrows() {
        assertThatThrownBy(() -> new HeuristicSwitchOracle(-0.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HeuristicSwitchOracle(1.1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
