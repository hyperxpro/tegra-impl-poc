package org.tegra.compute.ice;

import org.junit.jupiter.api.Test;
import org.tegra.api.EdgeDirection;
import org.tegra.compute.gas.VertexProgram;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class SwitchOracleTest {

    /** Minimal stub program for oracle testing. */
    private static final VertexProgram<Double, Double, Double> STUB_PROGRAM = new VertexProgram<>() {
        @Override public String name() { return "Stub"; }
        @Override public EdgeDirection gatherDirection() { return EdgeDirection.IN; }
        @Override public EdgeDirection scatterDirection() { return EdgeDirection.OUT; }
        @Override public Double gather(Double v, Double e, Double n) { return 0.0; }
        @Override public Double sum(Double a, Double b) { return a + b; }
        @Override public Double apply(Double current, Double gathered) { return current; }
        @Override public boolean scatter(Double updated, Double old, Double e) { return false; }
        @Override public Double identity() { return 0.0; }
    };

    @Test
    void testSwitchWhenHighActivity() {
        SwitchOracle oracle = SwitchOracle.defaultOracle();

        // 60 out of 100 active = 0.6 > 0.5 threshold
        boolean result = oracle.shouldSwitch(0, 60, 100, STUB_PROGRAM);
        assertThat(result).isTrue();
    }

    @Test
    void testNoSwitchWhenLowActivity() {
        SwitchOracle oracle = SwitchOracle.defaultOracle();

        // 10 out of 100 active = 0.1 < 0.5 threshold
        boolean result = oracle.shouldSwitch(0, 10, 100, STUB_PROGRAM);
        assertThat(result).isFalse();
    }

    @Test
    void testExactThresholdDoesNotSwitch() {
        SwitchOracle oracle = SwitchOracle.defaultOracle();

        // Exactly 0.5 should NOT switch (threshold is >)
        boolean result = oracle.shouldSwitch(0, 50, 100, STUB_PROGRAM);
        assertThat(result).isFalse();
    }

    @Test
    void testCustomThreshold() {
        SwitchOracle oracle = new HeuristicSwitchOracle(0.3);

        // 35 out of 100 = 0.35 > 0.3
        assertThat(oracle.shouldSwitch(0, 35, 100, STUB_PROGRAM)).isTrue();

        // 25 out of 100 = 0.25 < 0.3
        assertThat(oracle.shouldSwitch(0, 25, 100, STUB_PROGRAM)).isFalse();
    }

    @Test
    void testZeroTotalVertices() {
        SwitchOracle oracle = SwitchOracle.defaultOracle();

        // Should not switch on empty graph
        boolean result = oracle.shouldSwitch(0, 0, 0, STUB_PROGRAM);
        assertThat(result).isFalse();
    }
}
