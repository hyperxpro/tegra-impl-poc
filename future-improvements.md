# Tegra-J: Future Improvements

Items in this document are **not part of the paper specification**. They are potential enhancements identified during the research phase. None of these should be implemented until the core system achieves full specification parity.

---

## Storage Layer

### 1. C-Tree Compression (from Aspen, PLDI 2019)
Aspen's C-trees achieve significantly better compression than raw ART/pART by using difference encoding within tree nodes. The paper acknowledges Aspen achieves "slightly faster" retrieval due to better compression. Adopting C-tree encoding in pART leaves could reduce memory footprint by 2-5x for dense graphs.

### 2. HAMT Alternative Backend
The implementation guide (paper2.md) suggests starting with a Hash Array Mapped Trie instead of ART. While the paper uses ART, offering HAMT as a pluggable backend via SPI would enable users to choose the better fit for their workload. HAMTs have simpler implementation and similar structural sharing properties, at the cost of worse cache performance.

### 3. Tiered Storage (Hot/Warm/Cold)
The paper implements a two-tier model (memory/disk). A three-tier model with a warm tier (memory-mapped files without full materialization) could reduce eviction/materialization latency for frequently cycling snapshots.

### 4. Compressed Oops for Node Pointers
On 64-bit JVMs, pointers in ART nodes consume 8 bytes each. Using compressed OOPs or a custom pointer encoding (32-bit offsets within an arena) could halve the memory overhead of internal nodes.

### 5. Copy-on-Write Edge Grouping
Instead of storing edges individually in the edge pART, group edges by source vertex into blocks. Path-copying then amortizes across entire edge groups. This trades random-access performance for bulk-read performance, which suits GAS gather patterns.

---

## Computation Layer

### 6. Fine-Grained Lineage-Based Fault Tolerance
The paper explicitly states they do not support this: *"We currently do not support fine-grained lineage-based fault tolerance provided by Spark."* Implementing per-partition operation logging and replay would enable recovery without full checkpoint restoration.

### 7. Adaptive ICE Expansion Depth
The paper uses fixed 1-hop expansion. For algorithms with longer-range dependencies (e.g., BFS, k-hop), adaptive expansion depth (2-hop or k-hop based on algorithm metadata) could reduce the number of ICE iterations needed.

### 8. Neural Network Switching Model
The paper uses a random forest classifier for incremental-to-full switching decisions. A lightweight neural network (or gradient-boosted trees) trained on larger feature sets could improve switching accuracy, particularly for novel graph topologies.

### 9. Speculative ICE Execution
Run both incremental and full recomputation in parallel on separate virtual threads. Return whichever completes first. Wastes compute but guarantees optimal latency. Only viable when spare compute capacity exists.

### 10. Vectorized GAS Execution
Use the Java Vector API (incubator) to SIMD-accelerate gather/sum phases for algorithms with numeric vertex values (PageRank, CF). Node16 binary search in pART is also a candidate for SIMD acceleration.

---

## Distribution Layer

### 11. Dynamic Repartitioning
The paper uses static hash partitioning. As graphs evolve, partition skew may develop. Dynamic repartitioning (with minimal snapshot invalidation using persistent data structures) could maintain load balance.

### 12. Streaming Ingestion Pipeline
The paper's batch ingestion model requires explicit barrier coordination. A streaming ingestion pipeline with configurable windowing (time-based or count-based) could reduce ingestion latency and simplify the user model.

### 13. gRPC Transport
The core implementation uses raw Java NIO for simplicity. A gRPC transport plugin would provide: schema evolution (protobuf), mutual TLS, load balancing, and compatibility with service mesh infrastructure.

### 14. Kubernetes-Native Deployment
StatefulSet-based deployment with partition-to-pod affinity. Headless service for peer discovery. PersistentVolumeClaim for disk eviction storage.

### 15. Multi-Cluster Federation
Federate multiple Tegra-J clusters for geo-distributed graph analysis. Cross-cluster snapshots with causal consistency.

---

## API Layer

### 16. GraphQL Query Interface
Expose Timelapse operations via a GraphQL API for interactive exploration. Subscriptions for real-time snapshot notifications.

### 17. Python Client Bindings
Python client library for data scientists to interact with Tegra-J from Jupyter notebooks. Pandas DataFrame integration for query results.

### 18. Spark Connector
Since the original Tegra is built on Spark/GraphX, a Spark connector would enable existing GraphX users to migrate incrementally.

### 19. Property Graph Schema Validation
The paper uses untyped property maps. Adding optional schema validation (vertex/edge type definitions with required/optional properties and types) would catch errors early in production workloads.

---

## Observability

### 20. OpenTelemetry Integration
Distributed tracing for cross-partition GAS execution. Metrics export for snapshot counts, eviction rates, ICE switching decisions, and query latencies.

### 21. Admin Dashboard
Web UI showing cluster topology, partition health, snapshot timelines, memory/disk usage per partition, and active computations.

---

## Algorithms

### 22. Union-Find Connected Components
The paper explicitly uses label propagation for CC. Union-find is acknowledged as superior (the paper notes DD's union-find is "much superior"). Implementing union-find CC as an alternative would improve CC performance at the cost of not fitting cleanly into the GAS model.

### 23. Graph Neural Network Inference
GNN inference (e.g., GraphSAGE) can be expressed as GAS iterations. Adding a GNN vertex program would enable ML inference on temporal graph snapshots.

### 24. Temporal Pattern Mining
Algorithms that operate on the temporal dimension directly (motif detection, temporal path queries) rather than running static algorithms on individual snapshots.
