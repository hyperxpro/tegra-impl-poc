package org.tegra.benchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe collector for benchmark metrics: latency, memory, and throughput.
 */
public final class MetricsCollector {

    private final Map<String, List<Long>> latencies;
    private final Map<String, Long> memory;
    private final Map<String, long[]> throughput; // [count, nanos]

    public MetricsCollector() {
        this.latencies = new ConcurrentHashMap<>();
        this.memory = new ConcurrentHashMap<>();
        this.throughput = new ConcurrentHashMap<>();
    }

    private MetricsCollector(Map<String, List<Long>> latencies,
                             Map<String, Long> memory,
                             Map<String, long[]> throughput) {
        this.latencies = latencies;
        this.memory = memory;
        this.throughput = throughput;
    }

    /**
     * Records a latency sample for the given operation.
     *
     * @param operation the operation name
     * @param nanos     the latency in nanoseconds
     */
    public void recordLatency(String operation, long nanos) {
        latencies.computeIfAbsent(operation, k -> new CopyOnWriteArrayList<>()).add(nanos);
    }

    /**
     * Records a memory measurement.
     *
     * @param label the label for this measurement
     * @param bytes the number of bytes
     */
    public void recordMemory(String label, long bytes) {
        memory.put(label, bytes);
    }

    /**
     * Records a throughput measurement.
     *
     * @param operation the operation name
     * @param count     the number of items processed
     * @param nanos     the elapsed time in nanoseconds
     */
    public void recordThroughput(String operation, long count, long nanos) {
        throughput.put(operation, new long[]{count, nanos});
    }

    /**
     * Returns all recorded latencies, keyed by operation name.
     */
    public Map<String, List<Long>> getLatencies() {
        Map<String, List<Long>> copy = new HashMap<>();
        latencies.forEach((k, v) -> copy.put(k, new ArrayList<>(v)));
        return copy;
    }

    /**
     * Returns all recorded memory measurements.
     */
    public Map<String, Long> getMemory() {
        return new HashMap<>(memory);
    }

    /**
     * Returns all recorded throughput measurements.
     * Each entry maps operation name to [count, nanos].
     */
    public Map<String, long[]> getThroughput() {
        Map<String, long[]> copy = new HashMap<>();
        throughput.forEach((k, v) -> copy.put(k, new long[]{v[0], v[1]}));
        return copy;
    }

    /**
     * Returns a snapshot (deep copy) of the current state of this collector.
     */
    public MetricsCollector snapshot() {
        Map<String, List<Long>> latCopy = new ConcurrentHashMap<>();
        latencies.forEach((k, v) -> latCopy.put(k, new CopyOnWriteArrayList<>(v)));

        Map<String, Long> memCopy = new ConcurrentHashMap<>(memory);

        Map<String, long[]> tpCopy = new ConcurrentHashMap<>();
        throughput.forEach((k, v) -> tpCopy.put(k, new long[]{v[0], v[1]}));

        return new MetricsCollector(latCopy, memCopy, tpCopy);
    }
}
