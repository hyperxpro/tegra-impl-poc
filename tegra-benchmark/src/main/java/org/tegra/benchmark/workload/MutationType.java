package org.tegra.benchmark.workload;

/**
 * Type of mutations to generate in a workload.
 */
public enum MutationType {

    /** Only edge additions. */
    ADDITIONS_ONLY,

    /** Only edge deletions. */
    DELETIONS_ONLY,

    /** Mix of additions and deletions (approximately equal). */
    MIXED
}
