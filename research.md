# Tegra-J: A Java 21 Implementation of TEGRA for Apache-Grade Open Source Graph Analytics

## Research & Implementation Proposal

**Based on:** *TEGRA: Efficient Ad-Hoc Analytics on Evolving Graphs* (Iyer et al., NSDI 2021)
**Target Runtime:** Java 21+ (LTS)
**License Model:** Apache License 2.0
**Project Codename:** Tegra-J (working title; final ASF name TBD upon incubation)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Deep Analysis of the Tegra Paper](#2-deep-analysis-of-the-tegra-paper)
3. [Why Java 21](#3-why-java-21)
4. [System Architecture for Tegra-J](#4-system-architecture-for-tegra-j)
5. [Module Structure & Project Layout](#5-module-structure--project-layout)
6. [Component 1: Persistent Data Structures (tegra-pds)](#6-component-1-persistent-data-structures-tegra-pds)
7. [Component 2: DGSI — Distributed Graph Snapshot Index (tegra-store)](#7-component-2-dgsi--distributed-graph-snapshot-index-tegra-store)
8. [Component 3: Timelapse API (tegra-api)](#8-component-3-timelapse-api-tegra-api)
9. [Component 4: ICE Computation Engine (tegra-compute)](#9-component-4-ice-computation-engine-tegra-compute)
10. [Component 5: Distribution Layer (tegra-cluster)](#10-component-5-distribution-layer-tegra-cluster)
11. [Component 6: Serialization & Disk Eviction (tegra-serde)](#11-component-6-serialization--disk-eviction-tegra-serde)
12. [GAS Engine & Built-in Algorithms (tegra-algorithms)](#12-gas-engine--built-in-algorithms-tegra-algorithms)
13. [Memory Management Strategy](#13-memory-management-strategy)
14. [Java 21 Feature Utilization Map](#14-java-21-feature-utilization-map)
15. [API Design — The Public Contract](#15-api-design--the-public-contract)
16. [Integration Points & Ecosystem](#16-integration-points--ecosystem)
17. [Testing Strategy](#17-testing-strategy)
18. [Benchmarking & Evaluation Plan](#18-benchmarking--evaluation-plan)
19. [Phased Implementation Roadmap](#19-phased-implementation-roadmap)
20. [OSS Project Governance & Community](#20-oss-project-governance--community)
21. [Risk Analysis & Mitigations](#21-risk-analysis--mitigations)
22. [Appendix A: Key Data Structure Pseudocode](#appendix-a-key-data-structure-pseudocode)
23. [Appendix B: API Surface Draft](#appendix-b-api-surface-draft)
24. [Appendix C: Comparison with Existing Java Graph Libraries](#appendix-c-comparison-with-existing-java-graph-libraries)

---

## 1. Executive Summary

This document proposes a ground-up Java 21 implementation of the Tegra system described in the NSDI 2021 paper by Iyer et al. from UC Berkeley RISE Lab and Microsoft Research. Tegra addresses a critical gap in evolving graph analytics: the ability to perform **ad-hoc queries on arbitrary time windows** of a changing graph without full recomputation or prohibitive storage overhead.

The original Tegra was built on Apache Spark/GraphX in Scala. Our proposal, **Tegra-J**, is a standalone, dependency-minimal Java 21 implementation designed from the ground up to:

1. **Leverage Java 21's modern features** — virtual threads, sealed interfaces, records, Foreign Memory API, structured concurrency — to eliminate the GC concerns raised about JVM-based implementations while delivering performance competitive with native implementations.

2. **Be architected for open-source success** — clean module boundaries via JPMS, SPI-based extension points, Apache TinkerPop compatibility, and a layered API that serves both embedded single-node and distributed cluster deployments.

3. **Improve upon the original design** where the paper acknowledges limitations — specifically around purely streaming workloads (15% tree overhead), the switching heuristic (random forest classifier), and the tight coupling to Spark's execution model.

The core technical insight we preserve: **persistent data structures** (structural sharing across graph versions) combined with **incremental computation** (reusing intermediate results across snapshots) enables orders-of-magnitude improvement for ad-hoc temporal graph analytics.

Our implementation targets:
- **10M+ vertices, 1B+ edges** per node with 1000+ concurrent snapshots
- **Sub-second snapshot retrieval** (matching Tegra's 1-2s on Twitter/UK graphs)
- **18-30x speedup** on ad-hoc window operations vs. full recomputation (matching paper results)
- **Zero-copy snapshot access** via off-heap persistent data structures

---

## 2. Deep Analysis of the Tegra Paper

### 2.1 Problem Statement

Real-world graphs evolve continuously. Existing systems fall into silos:
- **Static graph engines** (GraphX, Pregel, PowerGraph): process one snapshot at a time, no temporal awareness.
- **Streaming engines** (Differential Dataflow, GraphBolt, Kineograph): keep a running query updated on the live graph, but cannot do ad-hoc historical queries.
- **Temporal engines** (Chronos, ImmortalGraph): optimized for sequential scans over a known time range, but require expensive preprocessing and cannot update results.

None support **ad-hoc window operations** — queries on arbitrary, non-predetermined, discontinuous time windows with computation reuse across windows.

### 2.2 Tegra's Three Pillars

#### Pillar 1: Timelapse Abstraction (Section 3 of paper)

Timelapse presents the evolving graph as a sequence of **immutable static snapshots**. This is both a user abstraction and a system optimization opportunity:

- **User perspective**: Work with familiar static graph APIs; time is just another dimension for retrieval.
- **System perspective**: Immutability enables concurrent access without locking, and the sequence structure enables incremental computation.

Key API operations: `save(id)`, `retrieve(id)`, `diff(snap, snap)`, `expand(candidates)`, `merge(snap, snap, func)`.

Critical insight from paper Section 3.1: By exposing entity lineage (the history of a vertex/edge across snapshots), graph-parallel phases can operate on the *evolution* of an entity rather than a single snapshot value, eliminating redundant messages. The paper demonstrates 5 out of 11 messages being duplicates in a simple degree computation across 3 snapshots (Figure 2).

#### Pillar 2: DGSI — Distributed Graph Snapshot Index (Section 5 of paper)

DGSI is the storage backbone. Three critical design decisions:

1. **Persistent Adaptive Radix Tree (pART)**: The paper reimplements PART in Scala with optimizations for graph storage. ART provides O(k) lookup (k = key length, not N), efficient range scans (important for edge retrieval by source vertex), and cache-friendly traversal. Path-copying adds persistence: modifying a leaf copies only O(log_256 n) ancestor nodes.

2. **Dual-tree storage**: Each partition stores a **vertex pART** (keyed by 64-bit vertex ID) and an **edge pART** (keyed by composite byte array: src + dst + discriminator). Prefix matching on edge keys retrieves all outgoing edges of a vertex — this is essential for the GAS gather phase.

3. **Version management via root pairs**: Every "version" (snapshot) is simply a pair of roots (vertex root, edge root) in the version map. Retrieval is a pointer lookup + tree traversal — O(1) to find the root, then O(key length) per entity access. This explains the sub-second retrieval (Table 3 in paper: TEGRA 1.34s vs. DD 30.2s for 200 snapshots of the Twitter graph).

**Memory management** (Section 5.4): LRU eviction writes version-specific subtrees to disk files. Shared nodes across versions share files. Only active snapshots are fully materialized in memory. The paper demonstrates storing 1000 snapshots of the UK graph (105M vertices, 3.7B edges) within cluster memory.

#### Pillar 3: ICE — Incremental Computation by Entity Expansion (Section 4 of paper)

ICE is Tegra's computation model for avoiding redundant work. Four phases:

1. **Initial execution**: Full computation; every iteration's state saved as a timelapse snapshot (e.g., `TWTR_1577869200_PR_1`, `TWTR_1577869200_PR_2`, ...).

2. **Bootstrap**: For a new graph snapshot, identify changed entities via `diff()`, expand to 1-hop neighborhood via `expand()`, run computation on this subgraph.

3. **Iterations**: At each iteration, diff the subgraph result against the stored timelapse iteration snapshot. Expand changed entities. Copy unchanged entities from stored state via `merge()`. This is the key correctness mechanism — ICE generates *identical* intermediate states as full re-execution (Section 4.2, proven by construction).

4. **Termination**: Not just when the subgraph converges, but when no entity needs state copied from stored snapshots. This handles cases where modifications cause more (or fewer) iterations than the initial execution.

**Critical property**: ICE is algorithm-independent. Any algorithm implemented in the GAS model can be made incremental without algorithm-specific refinement functions (unlike GraphBolt which requires custom `repropagate`, `retract`, `propagate` per algorithm).

**Switching heuristic** (Section 4.3): When incremental computation becomes more expensive than full re-execution (e.g., high-degree vertex changes cascade widely), a random forest classifier decides at iteration boundaries whether to switch. Features include active vertex count, average degree, partition activity, message counts, network transfer, and graph characteristics.

### 2.3 Key Experimental Results

| Metric | Result | Conditions |
|--------|--------|------------|
| Snapshot retrieval | 1.34s (TEGRA) vs 30.2s (DD) | 200 snapshots, Twitter graph |
| Ad-hoc single snapshot | 18-30x vs DD, 8-18x vs GraphBolt | 100 random windows, 0.1% change |
| Ad-hoc window (size 10) | 9-17x vs DD, 5-23x vs GraphBolt | Same setup |
| Memory growth | O(\|V\|) vs DD's O(\|E\|) per operator | 1M edge updates, Twitter |
| Scale | 50B edges, 1000 snapshots | Facebook synthetic data |
| Timelapse parallel | 36x speedup over GraphX serial | 20 snapshots, Twitter CC |
| Streaming (weakness) | DD/GraphBolt significantly faster | Online queries, continuous updates |
| Temporal (weakness) | 15% overhead vs Chronos | Tree structure overhead |

### 2.4 Limitations Acknowledged in the Paper

1. **Streaming performance**: Tegra accumulates batches (Spark-oriented); DD/GraphBolt push individual updates faster. Tegra is not designed for pure streaming.
2. **Tree overhead**: 15% slowdown vs. array-based Chronos for purely temporal (known-window) analysis.
3. **COST**: 32 cores to match single-threaded optimized implementation. Property graph overhead is real.
4. **Fault tolerance**: Coarse-grained only (Spark checkpoint). No fine-grained lineage recovery.
5. **Switching heuristic**: Simple random forest; paper acknowledges room for improvement.

### 2.5 What We Improve in Tegra-J

1. **Decouple from Spark**: The original Tegra is a "drop-in replacement for GraphX" — tightly coupled to Spark's execution model, RDD semantics, and JVM tuning. Tegra-J is standalone.
2. **Off-heap persistent data structures**: Address the GC concern directly. Tree nodes live outside the Java heap.
3. **Virtual thread-based distribution**: Replace Spark's bulk-synchronous model with lightweight, reactive message passing.
4. **Pluggable computation models**: GAS as default, but SPI allows Pregel, subgraph-centric, or custom models.
5. **Streaming bridge**: Optional adapter for Apache Flink or Kafka Streams ingestion, addressing the streaming weakness.
6. **Better eviction**: Tiered storage (heap → off-heap → local SSD → distributed FS) with configurable policies beyond simple LRU.

---

## 3. Why Java 21

The implementation guide (paper2.md) raises a valid concern:

> *"Java/Kotlin on JVM would work but the GC overhead may be problematic for a system that creates many short-lived tree nodes during path-copying."*

Java 21 fundamentally changes this equation. Here is our counter-argument:

### 3.1 The GC Problem is Solvable

**Path-copying creates O(log_256 n) new nodes per mutation.** For a graph with 100M vertices, that's ~4-5 nodes per mutation. In a batch of 10K mutations committed as one snapshot, that's ~50K new node objects. This is well within modern GC capabilities, but at scale (1000 snapshots, billions of edges), cumulative pressure matters.

Our answer is threefold:

1. **Foreign Memory API (JEP 454, finalized in Java 22, preview in 21)**: Tree nodes are allocated off-heap in `Arena`-managed `MemorySegment`s. The GC never sees them. Reference counting handles lifecycle. This is the same strategy used by Apache Arrow, Netty, and RocksDB's JNI layer.

2. **ZGC (Production-ready since Java 15)**: For the objects that *do* live on-heap (vertex properties, user-facing wrappers, message objects), ZGC delivers sub-millisecond pause times regardless of heap size. ZGC's concurrent relocation eliminates the stop-the-world pauses that would disrupt graph computation.

3. **Arena allocators for batch operations**: During a `commit()`, all path-copied nodes are allocated from a single arena. This gives locality, fast bulk deallocation, and zero fragmentation.

### 3.2 Java 21 Features That Directly Benefit Tegra

| Feature | JEP | Benefit for Tegra-J |
|---------|-----|---------------------|
| **Virtual Threads** | 444 (Final) | Millions of concurrent snapshot operations, lightweight per-partition message handlers, async ICE computation pipelines |
| **Structured Concurrency** | 462 (Preview) | Distributed snapshot barriers — fork per partition, join with timeout, cancel stragglers |
| **Scoped Values** | 446 (Preview) | Thread-local snapshot context propagation through GAS computation without parameter threading |
| **Record Classes** | 395 (Final) | Immutable value types for VertexId, EdgeId, SnapshotId, GraphDelta, Message. Compact, equals/hashCode for free |
| **Sealed Interfaces** | 409 (Final) | Closed type hierarchies for ART/HAMT node types (Node4, Node16, Node48, Node256, Leaf). Enables exhaustive pattern matching |
| **Pattern Matching** | 441 (Final) | Clean dispatch on node types during tree traversal, diff computation, message handling |
| **Foreign Function & Memory API** | 454 (Preview in 21, Final 22) | Off-heap tree node storage, zero-copy serialization, memory-mapped snapshot files |
| **Sequenced Collections** | 431 (Final) | Ordered snapshot sequences in Timelapse |
| **String Templates** | 430 (Preview) | Diagnostic logging, snapshot ID generation |
| **JPMS** | 261 (Since 9) | Strong module encapsulation; public API vs. internal implementation |

### 3.3 Ecosystem Advantages

- **Apache ecosystem alignment**: Spark, Flink, Kafka, Hadoop, TinkerPop, Arrow — all JVM-native. Integration is first-class, not via FFI.
- **Deployment ubiquity**: Every major cloud provider, container runtime, and enterprise environment has JVM support. No Rust toolchain required on target.
- **Contributor pool**: Java remains the most widely known language among data infrastructure engineers. ASF projects are predominantly JVM-based.
- **Observability**: JFR (Java Flight Recorder), async-profiler, JMX — mature production observability with zero-cost-when-off profiling.

### 3.4 Addressing Remaining Performance Gaps

The paper's Scala/Spark implementation already achieves 18-30x speedup over DD (Rust) for ad-hoc operations. Our Java 21 implementation should be *faster* than the original because:

1. **No Spark overhead**: Spark's scheduling, serialization, and RDD materialization are significant costs. The paper explicitly uses "barrier execution mode to avoid most Spark overheads" — we eliminate Spark entirely.
2. **Off-heap data structures**: Path-copying becomes allocation in pre-mapped memory regions, not GC-tracked object creation.
3. **Virtual threads eliminate thread-pool sizing problems**: The original's distributed GAS required careful thread management. Virtual threads make this trivial.
4. **JIT compilation**: HotSpot C2 / Graal JIT produce highly optimized native code for the tight loops in tree traversal and GAS phases. Profile-guided optimization kicks in after warmup.

---

## 4. System Architecture for Tegra-J

```
┌──────────────────────────────────────────────────────────────────────┐
│                        tegra-api (public)                            │
│  Timelapse<V,E>  │  GraphSnapshot<V,E>  │  GraphView<V,E>           │
│  TimelapseBuilder │  SnapshotId          │  GraphAlgorithm<V,E,R>   │
└──────┬───────────────────┬───────────────────────┬───────────────────┘
       │                   │                       │
       ▼                   ▼                       ▼
┌──────────────┐  ┌─────────────────┐  ┌───────────────────────┐
│ tegra-store  │  │ tegra-compute   │  │ tegra-algorithms      │
│              │  │                 │  │                       │
│  DGSI        │  │  ICE Engine     │  │  PageRank             │
│  VersionMap  │  │  GAS Framework  │  │  ConnectedComponents  │
│  SnapshotMgr │  │  DiffEngine     │  │  BeliefPropagation    │
│  EvictionMgr │  │  SwitchOracle   │  │  TriangleCount        │
│              │  │                 │  │  LabelPropagation     │
└──────┬───────┘  └────────┬────────┘  │  BFS / SSSP / k-hop  │
       │                   │           └───────────────────────┘
       ▼                   ▼
┌──────────────────────────────────────┐
│          tegra-pds (core)            │
│                                      │
│  PersistentART<K,V>                  │
│  PersistentHAMT<K,V>                │
│  Node4 / Node16 / Node48 / Node256  │
│  OffHeapArena                        │
│  PathCopyEngine                      │
│  DiffIterator                        │
└──────────────────────────────────────┘
       │                   │
       ▼                   ▼
┌──────────────┐  ┌─────────────────┐
│ tegra-serde  │  │ tegra-cluster   │
│              │  │                 │
│  Serializer  │  │  Partitioner    │
│  DiskStore   │  │  BarrierCoord   │
│  MMapLoader  │  │  MessageRouter  │
│  ArrowBridge │  │  VirtualRPC     │
└──────────────┘  └─────────────────┘
```

### 4.1 Dependency Direction

The dependency graph is strictly acyclic and layered:

```
tegra-api  →  tegra-store, tegra-compute, tegra-algorithms
tegra-store  →  tegra-pds, tegra-serde
tegra-compute  →  tegra-pds, tegra-store
tegra-algorithms  →  tegra-compute, tegra-api
tegra-cluster  →  tegra-store, tegra-compute, tegra-serde
tegra-serde  →  tegra-pds
tegra-pds  →  (no internal dependencies; only java.base + jdk.incubator.foreign)
```

### 4.2 Design Principles

1. **Embedded-first**: The core (tegra-pds + tegra-store + tegra-compute) runs in a single JVM with zero external dependencies. Distribution is an opt-in layer.
2. **Zero-copy by default**: Snapshot access returns views backed by the persistent tree, not materialized copies.
3. **Immutability as a type-level guarantee**: `GraphSnapshot<V,E>` is sealed and has no mutation methods. All mutation goes through `MutableGraphView`, which produces a new snapshot on `commit()`.
4. **SPI everywhere**: Storage backend, serialization format, partitioning strategy, eviction policy, and computation model are all pluggable via `java.util.ServiceLoader`.
5. **Generics for type safety**: `Timelapse<V, E>` where V is the vertex property type and E is the edge property type. No `Object` casts in user code.

---

## 5. Module Structure & Project Layout

```
tegra-j/
├── pom.xml                          # Parent POM (Maven multi-module)
├── bom/pom.xml                      # Bill of Materials for version alignment
│
├── tegra-pds/                       # Persistent Data Structures
│   ├── src/main/java/
│   │   └── org/tegra/pds/
│   │       ├── module-info.java
│   │       ├── art/                 # Adaptive Radix Tree (persistent)
│   │       │   ├── ArtNode.java     # Sealed interface
│   │       │   ├── Node4.java
│   │       │   ├── Node16.java
│   │       │   ├── Node48.java
│   │       │   ├── Node256.java
│   │       │   ├── Leaf.java
│   │       │   └── PersistentART.java
│   │       ├── hamt/                # Hash Array Mapped Trie (persistent)
│   │       │   ├── HamtNode.java
│   │       │   ├── BitmapIndexedNode.java
│   │       │   ├── ArrayNode.java
│   │       │   ├── CollisionNode.java
│   │       │   └── PersistentHAMT.java
│   │       ├── common/
│   │       │   ├── PathCopyResult.java  # record
│   │       │   ├── DiffEntry.java       # record
│   │       │   └── TrieIterator.java
│   │       └── memory/
│   │           ├── OffHeapArena.java
│   │           ├── NodeAllocator.java
│   │           └── RefCounted.java
│   └── src/test/java/
│
├── tegra-store/                     # DGSI implementation
│   ├── src/main/java/
│   │   └── org/tegra/store/
│   │       ├── module-info.java
│   │       ├── VersionMap.java
│   │       ├── SnapshotStore.java
│   │       ├── GraphPartition.java
│   │       ├── VertexStore.java
│   │       ├── EdgeStore.java
│   │       ├── SnapshotManager.java
│   │       ├── EvictionManager.java
│   │       ├── EvictionPolicy.java      # SPI interface
│   │       ├── LruEvictionPolicy.java
│   │       └── MutationLog.java
│   └── src/test/java/
│
├── tegra-api/                       # Public user-facing API
│   ├── src/main/java/
│   │   └── org/tegra/api/
│   │       ├── module-info.java
│   │       ├── Timelapse.java
│   │       ├── TimelapseBuilder.java
│   │       ├── GraphSnapshot.java       # sealed, read-only
│   │       ├── MutableGraphView.java
│   │       ├── GraphView.java           # sealed, parent
│   │       ├── SnapshotId.java          # record
│   │       ├── VertexId.java            # record
│   │       ├── EdgeId.java              # record
│   │       ├── Vertex.java              # record
│   │       ├── Edge.java                # record
│   │       ├── GraphDelta.java          # record
│   │       ├── PropertyMap.java
│   │       └── TegraConfig.java
│   └── src/test/java/
│
├── tegra-compute/                   # ICE + GAS computation engine
│   ├── src/main/java/
│   │   └── org/tegra/compute/
│   │       ├── module-info.java
│   │       ├── gas/
│   │       │   ├── GatherFunction.java
│   │       │   ├── ApplyFunction.java
│   │       │   ├── ScatterFunction.java
│   │       │   ├── VertexProgram.java
│   │       │   ├── GasEngine.java
│   │       │   └── GasContext.java
│   │       ├── ice/
│   │       │   ├── IceEngine.java
│   │       │   ├── DiffEngine.java
│   │       │   ├── NeighborhoodExpander.java
│   │       │   ├── SubgraphExtractor.java
│   │       │   ├── StateMerger.java
│   │       │   └── SwitchOracle.java
│   │       ├── pregel/
│   │       │   ├── PregelEngine.java
│   │       │   └── MessageCombiner.java
│   │       └── spi/
│   │           ├── ComputeEngine.java   # SPI interface
│   │           └── ComputeResult.java
│   └── src/test/java/
│
├── tegra-algorithms/                # Built-in graph algorithms
│   ├── src/main/java/
│   │   └── org/tegra/algorithms/
│   │       ├── module-info.java
│   │       ├── PageRank.java
│   │       ├── ConnectedComponents.java
│   │       ├── BeliefPropagation.java
│   │       ├── TriangleCount.java
│   │       ├── LabelPropagation.java
│   │       ├── ShortestPath.java
│   │       ├── BreadthFirstSearch.java
│   │       └── KHop.java
│   └── src/test/java/
│
├── tegra-serde/                     # Serialization & disk persistence
│   ├── src/main/java/
│   │   └── org/tegra/serde/
│   │       ├── module-info.java
│   │       ├── Serializer.java          # SPI interface
│   │       ├── BinarySerializer.java
│   │       ├── DiskSnapshotStore.java
│   │       ├── MMapSnapshotLoader.java
│   │       └── ArrowBridge.java         # Optional Apache Arrow integration
│   └── src/test/java/
│
├── tegra-cluster/                   # Distribution layer
│   ├── src/main/java/
│   │   └── org/tegra/cluster/
│   │       ├── module-info.java
│   │       ├── ClusterManager.java
│   │       ├── Partitioner.java         # SPI interface
│   │       ├── HashPartitioner.java
│   │       ├── TwoDPartitioner.java
│   │       ├── BarrierCoordinator.java
│   │       ├── MessageRouter.java
│   │       ├── RemoteVertexProxy.java
│   │       └── rpc/
│   │           ├── RpcServer.java
│   │           ├── RpcClient.java
│   │           └── VirtualThreadDispatcher.java
│   └── src/test/java/
│
├── tegra-benchmark/                 # JMH benchmarks
│   └── src/main/java/
│       └── org/tegra/benchmark/
│           ├── HamtBenchmark.java
│           ├── ArtBenchmark.java
│           ├── SnapshotRetrievalBenchmark.java
│           ├── PathCopyBenchmark.java
│           ├── IceBenchmark.java
│           └── EndToEndBenchmark.java
│
└── tegra-examples/                  # Usage examples
    └── src/main/java/
        └── org/tegra/examples/
            ├── QuickStart.java
            ├── TemporalAnalysis.java
            ├── AdHocWindowQuery.java
            ├── WhatIfAnalysis.java
            └── SlidingWindowPageRank.java
```

### 5.1 Build System

Maven with BOM for version management. Reasons:
- ASF standard (most Apache projects use Maven)
- Reproducible builds with `maven-enforcer-plugin`
- JPMS support via `maven-compiler-plugin` with `--module-path`
- Shade plugin for fat JARs in distributed mode
- JMH benchmarks via `jmh-maven-plugin`

Gradle wrapper also provided for contributors who prefer it.

### 5.2 Java Module System (JPMS)

Each module declares explicit dependencies and exports:

```java
// tegra-pds/src/main/java/module-info.java
module org.tegra.pds {
    exports org.tegra.pds.art;
    exports org.tegra.pds.hamt;
    exports org.tegra.pds.common;
    // memory package is internal — not exported
}

// tegra-api/src/main/java/module-info.java
module org.tegra.api {
    requires org.tegra.pds;
    requires org.tegra.store;
    exports org.tegra.api;
}
```

This enforces that users can only depend on `tegra-api` and `tegra-algorithms` — internal data structures are encapsulated.

---

## 6. Component 1: Persistent Data Structures (tegra-pds)

This is the most technically critical module. It implements the persistent (functional) tree structures that underpin DGSI's structural sharing.

### 6.1 Strategy: HAMT First, ART Second

Following the paper's own recommendation (Section 5, "Simplification for v1"):

**Phase 1 — Persistent HAMT (Hash Array Mapped Trie):**
- Well-understood data structure (Bagwell 2001; used by Clojure, Scala, Haskell)
- 32-way branching at each level (5 bits of hash per level)
- Structural sharing via path-copying: modify a leaf → copy O(log_32 n) = O(7) nodes for 1B entries
- Bitmap indexing for sparse nodes keeps memory compact
- Java implementations exist for reference (Capsule by Steindorfer, paguro by GlenPeterson)

**Phase 2 — Persistent ART (Adaptive Radix Tree):**
- The paper's actual data structure choice for production
- Variable fan-out (4, 16, 48, 256) adapts to key distribution
- Better cache performance than HAMT for range scans (important for edge retrieval)
- O(k) lookup where k = key length (not dependent on N)
- Prefix compression reduces tree depth

### 6.2 HAMT Design

```java
public sealed interface HamtNode<K, V>
    permits BitmapIndexedNode, ArrayNode, CollisionNode, EmptyNode {

    HamtNode<K, V> put(K key, V value, int hash, int shift, MutationContext ctx);
    HamtNode<K, V> remove(K key, int hash, int shift, MutationContext ctx);
    V get(K key, int hash, int shift);
    DiffIterator<K, V> diff(HamtNode<K, V> other);
    int size();
}
```

**Node types:**

| Type | When Used | Layout |
|------|-----------|--------|
| `EmptyNode` | Singleton empty trie | No fields |
| `BitmapIndexedNode` | < 16 children at this level | `int bitmap` + `Object[] contents` (interleaved key/value/subnode) |
| `ArrayNode` | >= 16 children | `HamtNode[32] children` (direct-indexed, no bitmap) |
| `CollisionNode` | Hash collision at this level | `Entry<K,V>[] entries` (linear scan, rare) |

**Path-copying with transient mutation optimization:**

During a `commit()` that batches many mutations, we use a `MutationContext` (inspired by Clojure's `edit` field):
- Nodes created during the current batch share a `MutationContext` token.
- Mutations to nodes with the current token are done **in-place** (no copy needed).
- Once `commit()` is called, the token is invalidated, and all nodes become persistent.
- This optimization is critical: it means batching 10K mutations creates only the final structural diff, not 10K intermediate copies.

```java
public record MutationContext(Thread owner, long epoch) {
    // In-place mutation is allowed only if the node's context matches
    // the current context AND we're on the owning thread.
    public boolean isEditable() {
        return owner == Thread.currentThread();
    }
}
```

### 6.3 ART Design

The Adaptive Radix Tree uses 4 node types based on fan-out:

```java
public sealed interface ArtNode<V>
    permits Node4, Node16, Node48, Node256, ArtLeaf {
    // ...
}

// Node4: Up to 4 children. Keys stored in sorted array, binary search.
public final class Node4<V> implements ArtNode<V> {
    byte[] keys;     // length 4
    ArtNode<V>[] children; // length 4
    int count;
    byte[] prefix;   // path compression
    int prefixLen;
}

// Node16: Up to 16 children. Keys stored in sorted array.
// On x86, SIMD comparison for key lookup (via Vector API or manual unrolling).

// Node48: Up to 48 children. 256-element index array mapping byte → child slot.
// Avoids scanning; direct lookup. Slot array has 48 entries.

// Node256: Direct mapping. children[byte] = child. No key search needed.

// ArtLeaf: Terminal node storing the full key and value.
```

**Persistence via path-copying** is identical to HAMT: modifying a leaf copies the path from leaf to root. Growth/shrink operations (Node4 → Node16, etc.) are handled during path-copying.

**Prefix compression** is essential for byte-key efficiency. Shared prefixes are stored once in the nearest common ancestor rather than replicated in every descendant.

### 6.4 Off-Heap Memory Architecture

The critical innovation for Java: store tree nodes off-heap to avoid GC pressure.

```java
public final class OffHeapArena implements AutoCloseable {
    private final Arena arena;  // java.lang.foreign.Arena
    private final MemorySegment segment;
    private long offset;

    public OffHeapArena(long capacityBytes) {
        this.arena = Arena.ofShared();
        this.segment = arena.allocate(capacityBytes);
        this.offset = 0;
    }

    public long allocate(int size) {
        long addr = offset;
        offset += align(size, 8); // 8-byte alignment
        return addr;
    }

    public MemorySegment slice(long offset, long size) {
        return segment.asSlice(offset, size);
    }

    @Override
    public void close() {
        arena.close();
    }
}
```

**Node layout in off-heap memory (example: HAMT BitmapIndexedNode):**

```
Offset  Size   Field
0       4      bitmap (int)
4       4      count (int)
8       4      mutationEpoch (int)
12      4      reserved/padding
16      8*N    child pointers (long offsets into arena, or tagged pointers)
16+8N   ?      inline leaf data (for small values)
```

**Tagged pointers** distinguish node types without virtual dispatch:
- Bits 0-1: node type tag (00 = BitmapIndexed, 01 = Array, 10 = Collision, 11 = Leaf)
- Bits 2-63: offset into arena

This eliminates polymorphic call overhead in tight traversal loops — a common source of JIT deoptimization in tree-heavy code.

### 6.5 Diff Algorithm

The `diff()` operation is central to ICE. For persistent trees, diffing is highly efficient because **structurally shared subtrees are referentially equal** — a pointer comparison skips entire unchanged subtrees.

```
diff(nodeA, nodeB):
    if nodeA == nodeB:       // Same reference → identical subtree
        return EMPTY_DIFF    // Skip entirely (this is the key optimization)
    if both are leaves:
        return MODIFIED(nodeA.key, nodeA.value, nodeB.value)
    for each child position:
        childA = nodeA.child(pos)
        childB = nodeB.child(pos)
        if childA == null and childB != null:
            yield all entries in childB as ADDED
        elif childA != null and childB == null:
            yield all entries in childA as REMOVED
        elif childA != childB:        // Different references → recurse
            yield diff(childA, childB)
        // else: childA == childB → skip (shared structure)
```

Complexity: O(d) where d = number of *changed* entries, not total entries. For Tegra's use case (0.1% change between snapshots), this means diffing a 100M-vertex graph examines only ~100K nodes.

---

## 7. Component 2: DGSI — Distributed Graph Snapshot Index (tegra-store)

### 7.1 Core Abstractions

```java
/**
 * A single partition's graph storage. Each partition stores
 * a vertex tree and an edge tree, both persistent.
 */
public final class GraphPartition<V, E> {
    private final PersistentART<Long, VertexData<V>> vertexTree;
    private final PersistentART<byte[], EdgeData<E>> edgeTree;
    private final VersionMap versions;
    private final EvictionManager eviction;
    private final MutationLog mutationLog;
    // ...
}
```

### 7.2 Version Map

The version map is itself a persistent data structure — a HAMT keyed by `SnapshotId` (byte array) storing root pairs:

```java
public record VersionRoot<V, E>(
    ArtNode<VertexData<V>> vertexRoot,
    ArtNode<EdgeData<E>> edgeRoot,
    Instant timestamp,
    long mutationLogOffset  // pointer to mutations since previous snapshot
) {}

public final class VersionMap<V, E> {
    private final PersistentHAMT<SnapshotId, VersionRoot<V, E>> roots;

    public GraphSnapshot<V, E> get(SnapshotId id) { /* O(1) lookup */ }
    public SnapshotId commit(VersionRoot<V, E> root) { /* atomic insert */ }
    public List<SnapshotId> range(SnapshotId prefix) { /* prefix scan */ }
    public VersionRoot<V, E> branch(SnapshotId source) { /* copy root pair */ }
}
```

### 7.3 Edge Key Design

Following the paper (Section 5.2), edge keys are composite byte arrays enabling prefix-based retrieval:

```java
/**
 * Edge key layout (16 bytes):
 *   bytes 0-7:  source vertex ID (long, big-endian)
 *   bytes 8-15: destination vertex ID (long, big-endian)
 *
 * This layout enables:
 *   - prefix(8 bytes) → all outgoing edges from a source vertex
 *   - full key → specific edge lookup
 *   - range(srcA, srcB) → all edges from vertices in range
 *
 * For multigraphs, extend to 18 bytes with a 2-byte discriminator.
 */
public record EdgeId(long src, long dst, short discriminator) {
    public byte[] toKey() {
        byte[] key = new byte[18];
        ByteBuffer.wrap(key)
            .putLong(src)
            .putLong(dst)
            .putShort(discriminator);
        return key;
    }
}
```

The ART's prefix matching capability makes "retrieve all outgoing edges of vertex X" a prefix scan on the first 8 bytes — O(outDegree) with no full-tree scan.

### 7.4 Snapshot Lifecycle

```
                    ┌─────────────┐
                    │  No Version  │
                    └──────┬──────┘
                           │ branch(existingId) or initial load
                           ▼
                    ┌─────────────┐
                    │  TRANSIENT   │  ← mutable, not visible to others
                    │  (working)   │    in-place updates via MutationContext
                    └──────┬──────┘
                           │ commit(id)
                           ▼
                    ┌─────────────┐
                    │  COMMITTED   │  ← immutable, visible, in-memory
                    │  (active)    │    fully materialized, fast access
                    └──────┬──────┘
                           │ LRU timeout or memory pressure
                           ▼
                    ┌─────────────┐
                    │   EVICTED    │  ← serialized subtrees on disk
                    │   (cold)     │    roots still in-memory (small)
                    └──────┬──────┘
                           │ retrieve(id) — demand-load
                           ▼
                    ┌─────────────┐
                    │  COMMITTED   │  ← re-materialized from disk
                    │  (warm)      │
                    └─────────────┘
```

### 7.5 Commit Protocol (Single-Node)

```java
public SnapshotId commit(MutableGraphView<V, E> working) {
    // 1. Finalize the mutation context (freeze in-place mutations)
    working.mutationContext().freeze();

    // 2. The working view's tree roots ARE the new version
    //    (path-copying already happened during mutations)
    var root = new VersionRoot<>(
        working.vertexRoot(),
        working.edgeRoot(),
        Instant.now(),
        mutationLog.currentOffset()
    );

    // 3. Generate snapshot ID
    var id = idGenerator.next(working.graphId());

    // 4. Atomic insert into version map
    versions.put(id, root);

    // 5. Update eviction tracking
    eviction.onAccess(id);

    return id;
}
```

### 7.6 Distributed Commit Protocol

For distributed deployments, all partitions must commit at the same logical time to ensure a consistent distributed snapshot. We implement a two-phase barrier:

```
Coordinator                 Partition 1    Partition 2    Partition N
    │                           │              │              │
    │── PREPARE_COMMIT(batchId)──►              │              │
    │                           │──PREPARE_COMMIT──►           │
    │                           │              │──PREPARE_COMMIT──►
    │                           │              │              │
    │◄── READY(localRoot) ──────│              │              │
    │◄── READY(localRoot) ─────────────────────│              │
    │◄── READY(localRoot) ────────────────────────────────────│
    │                           │              │              │
    │   (all partitions ready)  │              │              │
    │                           │              │              │
    │── COMMIT(snapshotId) ─────►              │              │
    │                           │── COMMIT ────►              │
    │                           │              │── COMMIT ────►
    │                           │              │              │
    │◄── ACK ───────────────────│              │              │
    │◄── ACK ──────────────────────────────────│              │
    │◄── ACK ─────────────────────────────────────────────────│
```

Implemented using structured concurrency:

```java
public SnapshotId distributedCommit(BatchId batchId) throws Exception {
    var id = idGenerator.next(graphId);

    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
        // Fork one virtual thread per partition
        List<Subtask<VersionRoot>> tasks = partitions.stream()
            .map(p -> scope.fork(() -> p.prepareCommit(batchId)))
            .toList();

        scope.join();           // Wait for all partitions
        scope.throwIfFailed();  // Propagate first failure

        // All prepared — now commit atomically
        for (int i = 0; i < partitions.size(); i++) {
            partitions.get(i).finalizeCommit(id, tasks.get(i).get());
        }
    }
    return id;
}
```

---

## 8. Component 3: Timelapse API (tegra-api)

### 8.1 Core Types

```java
/**
 * A Timelapse represents the evolution of a graph over time.
 * It is the primary user-facing abstraction in Tegra-J.
 *
 * @param <V> vertex property type
 * @param <E> edge property type
 */
public final class Timelapse<V, E> implements Iterable<GraphSnapshot<V, E>> {

    /** Save the current graph state as a new snapshot. */
    public SnapshotId save(String id);

    /** Retrieve a snapshot by exact ID. */
    public GraphSnapshot<V, E> retrieve(SnapshotId id);

    /** Retrieve snapshots matching a prefix (e.g., "TWTR_15778"). */
    public List<GraphSnapshot<V, E>> retrieve(String prefix);

    /** Retrieve snapshots in a range. */
    public List<GraphSnapshot<V, E>> range(SnapshotId from, SnapshotId to);

    /** Compute the difference between two snapshots. */
    public GraphDelta<V, E> diff(SnapshotId a, SnapshotId b);

    /** Branch from an existing snapshot for what-if analysis. */
    public MutableGraphView<V, E> branch(SnapshotId source);

    /** Run an algorithm on a single snapshot. */
    public <R> R run(SnapshotId id, GraphAlgorithm<V, E, R> algorithm);

    /** Run an algorithm across multiple snapshots in parallel. */
    public <R> Map<SnapshotId, R> map(
        List<SnapshotId> snapshots,
        GraphAlgorithm<V, E, R> algorithm);

    /** Sliding window query. */
    public <R> List<R> window(
        SnapshotId start, SnapshotId end,
        Duration stride, GraphAlgorithm<V, E, R> algorithm);

    /** Run an algorithm incrementally (ICE) across a snapshot sequence. */
    public <R> Map<SnapshotId, R> mapIncremental(
        List<SnapshotId> snapshots,
        GraphAlgorithm<V, E, R> algorithm);
}
```

### 8.2 Snapshot ID Convention

Following the paper's hierarchical naming scheme:

```
Format: {GRAPH_ID}_{UNIX_EPOCH}[_{ALGO_ID}_{ITERATION}]

Examples:
  TWTR_1577869200                    — Graph snapshot at 9:00 AM
  TWTR_1577869200_PR_1               — PageRank iteration 1 on that snapshot
  TWTR_1577869200_PR_2               — PageRank iteration 2
  TWTR_1577872800                    — Graph snapshot at 10:00 AM
  TWTR_1577872800_CC_1               — Connected Components iteration 1

Queries:
  retrieve("TWTR_")                  — All snapshots of the Twitter graph
  retrieve("TWTR_1577869200_PR_")    — All PageRank iterations on the 9 AM snapshot
  range("TWTR_1577869200", "TWTR_1577872800") — All snapshots between 9-10 AM
```

```java
public record SnapshotId(byte[] raw) implements Comparable<SnapshotId> {

    public static SnapshotId of(String graphId, Instant timestamp) {
        return new SnapshotId(
            (graphId + "_" + timestamp.getEpochSecond()).getBytes(UTF_8));
    }

    public static SnapshotId ofIteration(SnapshotId base, String algoId, int iter) {
        return new SnapshotId(
            (new String(base.raw, UTF_8) + "_" + algoId + "_" + iter)
                .getBytes(UTF_8));
    }

    public boolean hasPrefix(SnapshotId prefix) {
        return Arrays.mismatch(raw, prefix.raw) >= prefix.raw.length;
    }

    @Override
    public int compareTo(SnapshotId other) {
        return Arrays.compare(raw, other.raw);
    }
}
```

### 8.3 Graph Snapshot (Immutable View)

```java
public sealed interface GraphView<V, E>
    permits GraphSnapshot, MutableGraphView {

    long vertexCount();
    long edgeCount();
    Optional<Vertex<V>> vertex(long id);
    Stream<Vertex<V>> vertices();
    Stream<Edge<E>> outEdges(long vertexId);
    Stream<Edge<E>> inEdges(long vertexId);
    Stream<Edge<E>> edges();
}

/**
 * An immutable, point-in-time view of the graph.
 * Thread-safe; zero-copy (backed by persistent tree traversal).
 */
public final class GraphSnapshot<V, E> implements GraphView<V, E> {
    private final ArtNode<VertexData<V>> vertexRoot;
    private final ArtNode<EdgeData<E>> edgeRoot;
    private final SnapshotId id;
    // No mutation methods. Compile-time guarantee of immutability.
}

/**
 * A mutable working copy created via branch().
 * Not thread-safe; must be used by a single thread or under external sync.
 */
public final class MutableGraphView<V, E> implements GraphView<V, E> {
    public void addVertex(long id, V properties);
    public void removeVertex(long id);
    public void addEdge(long src, long dst, E properties);
    public void removeEdge(long src, long dst);
    public void setVertexProperty(long id, V properties);
    public void setEdgeProperty(long src, long dst, E properties);
}
```

---

## 9. Component 4: ICE Computation Engine (tegra-compute)

### 9.1 GAS Framework

The Gather-Apply-Scatter model from PowerGraph, adapted for Tegra's persistent snapshots:

```java
/**
 * A vertex program in the GAS model.
 * Implementations define the three phases of graph-parallel computation.
 *
 * @param <V> vertex value type
 * @param <E> edge value type
 * @param <M> message type (gathered/scattered between vertices)
 */
public interface VertexProgram<V, E, M> {

    /** Direction of edges to gather from. */
    EdgeDirection gatherDirection();

    /** Direction of edges to scatter to. */
    EdgeDirection scatterDirection();

    /** Gather: compute a partial message from one edge. */
    M gather(V vertexValue, E edgeValue, V neighborValue);

    /** Sum: combine two gathered messages (associative, commutative). */
    M sum(M a, M b);

    /** Apply: update vertex value using the gathered message. */
    V apply(V currentValue, M gathered);

    /** Scatter: determine if a neighbor should be activated. */
    boolean scatter(V updatedValue, V oldValue, E edgeValue);

    /** Initial message for vertices with no incoming messages. */
    M identity();

    /** Convergence check. */
    default boolean hasConverged(V oldValue, V newValue) {
        return Objects.equals(oldValue, newValue);
    }
}
```

### 9.2 GAS Engine Execution (Single Snapshot)

```java
public final class GasEngine<V, E> {

    public <M> GraphSnapshot<V, E> execute(
            GraphSnapshot<V, E> snapshot,
            VertexProgram<V, E, M> program,
            int maxIterations,
            Timelapse<V, E> stateTimelapse  // stores iteration states
    ) {
        var working = snapshot.asMutable();
        Set<Long> activeVertices = allVertexIds(working);

        for (int iter = 0; iter < maxIterations && !activeVertices.isEmpty(); iter++) {

            // GATHER: For each active vertex, gather from neighbors
            Map<Long, M> messages = gatherPhase(working, program, activeVertices);

            // APPLY: Update vertex values
            Set<Long> changedVertices = applyPhase(working, program, messages);

            // SCATTER: Determine next active set
            activeVertices = scatterPhase(working, program, changedVertices);

            // Save iteration state to timelapse
            if (stateTimelapse != null) {
                stateTimelapse.save(
                    SnapshotId.ofIteration(snapshot.id(), program.name(), iter));
            }
        }

        return working.commit();
    }
}
```

### 9.3 ICE Engine (Incremental Computation)

This is the core of Tegra's performance advantage. Implements Section 4.2 of the paper:

```java
public final class IceEngine<V, E> {

    /**
     * Execute a vertex program incrementally on a new snapshot,
     * using stored iteration states from a previous execution.
     */
    public <M> GraphSnapshot<V, E> executeIncremental(
            GraphSnapshot<V, E> newSnapshot,
            VertexProgram<V, E, M> program,
            Timelapse<V, E> previousStates,  // timelapse of previous execution
            SwitchOracle switchOracle
    ) {
        // === BOOTSTRAP PHASE ===

        // 1. Diff the new snapshot against the first stored iteration state
        SnapshotId prevBaseId = previousStates.first().id();
        GraphDelta<V, E> delta = newSnapshot.diff(previousStates.retrieve(prevBaseId));

        // 2. Identify affected entities
        Set<Long> affectedVertices = delta.changedVertexIds();
        for (var edgeChange : delta.changedEdges()) {
            affectedVertices.add(edgeChange.src());
            affectedVertices.add(edgeChange.dst());
        }

        // 3. Expand to 1-hop neighborhood
        Set<Long> subgraphVertices = expandOneHop(newSnapshot, affectedVertices);

        // 4. Bootstrap: copy state from previous result for non-affected vertices
        var working = newSnapshot.asMutable();
        mergeState(working, previousStates.last(), subgraphVertices);

        // === ITERATION PHASE ===

        int iter = 0;
        boolean converged = false;

        while (!converged) {
            // Check switch oracle — should we abandon incremental?
            if (switchOracle.shouldSwitch(iter, subgraphVertices.size(),
                    working.vertexCount(), program)) {
                return gasEngine.execute(newSnapshot, program,
                    program.maxIterations() - iter, null);
            }

            // Run GAS only on the subgraph
            Map<Long, M> messages = gatherPhase(working, program, subgraphVertices);
            Set<Long> changedVertices = applyPhase(working, program, messages);

            // Diff against stored iteration state
            SnapshotId prevIterationId =
                SnapshotId.ofIteration(prevBaseId, program.name(), iter);

            if (previousStates.has(prevIterationId)) {
                GraphSnapshot<V, E> prevIteration =
                    previousStates.retrieve(prevIterationId);

                // Copy state for vertices NOT in subgraph from previous iteration
                mergeState(working, prevIteration, subgraphVertices);

                // Find new subgraph: diff current subgraph against stored state
                GraphDelta<V, E> iterDelta = diffSubgraph(working, prevIteration);
                Set<Long> newAffected = iterDelta.changedVertexIds();
                subgraphVertices = expandOneHop(working, newAffected);
            }

            // Scatter to find next active set within subgraph
            subgraphVertices = scatterPhase(working, program, changedVertices);

            iter++;

            // === TERMINATION CHECK ===
            // Converged when: subgraph has no active vertices AND
            // no entity needs state copied from stored snapshot
            converged = subgraphVertices.isEmpty() &&
                        (iter >= previousIterationCount || noStateToCopy(working, previousStates, iter));
        }

        return working.commit();
    }
}
```

### 9.4 Switch Oracle

The paper uses a random forest classifier. We provide an SPI with a default heuristic-based implementation and an optional ML-based one:

```java
/**
 * Decides whether ICE should switch to full re-execution.
 * SPI interface — users can provide custom implementations.
 */
public interface SwitchOracle {

    boolean shouldSwitch(
        int currentIteration,
        int activeVertexCount,
        long totalVertexCount,
        VertexProgram<?, ?, ?> program
    );

    /**
     * Default implementation: switch when active vertices exceed
     * a dynamic threshold based on graph characteristics.
     */
    static SwitchOracle defaultOracle() {
        return new HeuristicSwitchOracle();
    }

    /**
     * Oracle based on pre-trained model.
     * Loaded from a model file via SPI.
     */
    static SwitchOracle fromModel(Path modelPath) {
        return new ModelBasedSwitchOracle(modelPath);
    }
}
```

### 9.5 Diff Engine

The diff engine is the bridge between DGSI's structural sharing and ICE's incremental computation:

```java
public final class DiffEngine<V, E> {

    /**
     * Compute the structural difference between two snapshots.
     * Leverages referential equality of shared subtrees for O(changes) complexity.
     */
    public GraphDelta<V, E> diff(GraphSnapshot<V, E> a, GraphSnapshot<V, E> b) {
        var vertexDiff = a.vertexRoot().diff(b.vertexRoot());
        var edgeDiff = a.edgeRoot().diff(b.edgeRoot());
        return new GraphDelta<>(vertexDiff, edgeDiff);
    }
}

public record GraphDelta<V, E>(
    List<DiffEntry<Long, VertexData<V>>> vertexChanges,
    List<DiffEntry<byte[], EdgeData<E>>> edgeChanges
) {
    public Set<Long> changedVertexIds() { /* ... */ }
    public Set<Long> affectedVertexIds() {
        // Includes both endpoints of changed edges
    }
}

public record DiffEntry<K, V>(K key, V oldValue, V newValue, ChangeType type) {
    public enum ChangeType { ADDED, REMOVED, MODIFIED }
}
```

---

## 10. Component 5: Distribution Layer (tegra-cluster)

### 10.1 Architecture

Tegra-J's distribution layer is designed to be simpler and more lightweight than Spark:

```
┌──────────────────────────────────────────────────────────────┐
│                     Coordinator Node                         │
│  ┌─────────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ BarrierCoord    │  │ PartitionMap │  │ QueryRouter   │  │
│  └─────────────────┘  └──────────────┘  └───────────────┘  │
└──────────┬──────────────────┬──────────────────┬────────────┘
           │                  │                  │
     ┌─────▼──────┐    ┌─────▼──────┐    ┌─────▼──────┐
     │ Worker 1   │    │ Worker 2   │    │ Worker N   │
     │            │    │            │    │            │
     │ Partition 0│    │ Partition 1│    │ Partition M│
     │ DGSI local │    │ DGSI local │    │ DGSI local │
     │ GAS local  │    │ GAS local  │    │ GAS local  │
     │            │    │            │    │            │
     │ VThread RPC│◄──►│ VThread RPC│◄──►│ VThread RPC│
     └────────────┘    └────────────┘    └────────────┘
```

### 10.2 Partitioning Strategy

```java
public sealed interface Partitioner
    permits HashPartitioner, TwoDPartitioner, CustomPartitioner {

    /** Given a vertex ID, return the partition (worker) index. */
    int partitionVertex(long vertexId);

    /** Given an edge, return the partition(s) that store it. */
    int[] partitionEdge(long srcId, long dstId);
}

/**
 * Default: consistent hashing on vertex ID.
 * Each vertex lives on exactly one partition.
 * Edges are stored on the source vertex's partition (for GAS gather efficiency).
 */
public final class HashPartitioner implements Partitioner {
    private final int numPartitions;

    @Override
    public int partitionVertex(long vertexId) {
        return Long.hashCode(vertexId) % numPartitions;
    }
}
```

### 10.3 Virtual Thread RPC

Each worker runs a virtual-thread-based RPC server. Cross-partition communication (needed for GAS gather/scatter on edges that span partitions) uses lightweight message passing:

```java
public final class VirtualThreadDispatcher {
    private final ServerSocket server;
    private final ExecutorService vthreadPool =
        Executors.newVirtualThreadPerTaskExecutor();

    public void start() {
        vthreadPool.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                Socket conn = server.accept();
                // Each connection handled by a virtual thread
                vthreadPool.submit(() -> handleConnection(conn));
            }
        });
    }

    private void handleConnection(Socket conn) {
        // Deserialize message, dispatch to local DGSI/GAS, send response
        // Virtual threads can block on I/O without thread pool exhaustion
    }
}
```

### 10.4 Distributed GAS Execution

```
Superstep N:
  1. Each partition runs GATHER locally for its vertices
     - For cross-partition edges: send GatherRequest to remote partition
     - Remote partition responds with neighbor vertex value
     - Virtual thread blocks on response (cheap with virtual threads)

  2. Each partition runs APPLY locally (purely local)

  3. Each partition runs SCATTER locally
     - For cross-partition edges: send updated vertex value to remote
     - Remote partition queues the value for next superstep's gather

  4. Barrier: all partitions report active vertex count
     - If global active count == 0 → converged
     - Else → next superstep
```

### 10.5 Distributed ICE

Cross-partition ICE requires one additional round of communication: the 1-hop neighborhood expansion may cross partition boundaries.

```
Bootstrap:
  1. Each partition computes local diff
  2. For vertices whose 1-hop neighbors are on other partitions:
     send EXPAND_REQUEST to those partitions
  3. Remote partitions include those vertices in their subgraph
  4. Proceed with local ICE computation

Iterations:
  Same as single-node ICE, but scatter/gather cross partition boundaries
  using the same RPC mechanism as distributed GAS.
```

---

## 11. Component 6: Serialization & Disk Eviction (tegra-serde)

### 11.1 Binary Format

We use a custom binary format (not JSON, not Protobuf) optimized for ART/HAMT subtree serialization:

```
File format: .tsnap (Tegra Snapshot)

Header (32 bytes):
  magic:     4 bytes  "TSNP"
  version:   2 bytes  format version
  flags:     2 bytes  (compressed, encrypted, etc.)
  nodeCount: 8 bytes  number of nodes in this subtree
  rootType:  1 byte   node type of the root
  keyLen:    1 byte   key length (for ART)
  reserved:  14 bytes

Node entries (variable length, sequential):
  Each node:
    type:     1 byte   (Node4=0, Node16=1, Node48=2, Node256=3, Leaf=4)
    size:     4 bytes  total entry size
    data:     variable (type-specific layout)

  Node4 data:
    count:    1 byte
    prefix:   variable (prefixLen + prefix bytes)
    keys:     count bytes
    children: count * 8 bytes (file offsets to child entries)

  Leaf data:
    keyLen:   2 bytes
    key:      keyLen bytes
    valueLen: 4 bytes
    value:    valueLen bytes
```

### 11.2 Memory-Mapped Loading

Evicted snapshots are loaded on-demand using memory-mapped files:

```java
public final class MMapSnapshotLoader<V, E> {

    public GraphSnapshot<V, E> load(SnapshotId id, Path file) {
        // Memory-map the file — OS manages physical memory paging
        try (var arena = Arena.ofShared()) {
            var channel = FileChannel.open(file, READ);
            var mapped = channel.map(MapMode.READ_ONLY, 0, channel.size(), arena);

            // Reconstruct tree from mapped memory
            // Nodes reference memory-mapped segments, no deserialization needed
            var root = reconstructTree(mapped);
            return new GraphSnapshot<>(root.vertexRoot(), root.edgeRoot(), id);
        }
    }
}
```

This means evicted snapshots can be accessed with zero deserialization overhead — the OS pages in file data on demand via the page cache.

### 11.3 Tiered Eviction

```
Tier 0: Java Heap (small objects, vertex properties, metadata)
        Access: nanoseconds
        Size: limited by -Xmx

Tier 1: Off-Heap Direct Memory (tree nodes, structural data)
        Access: nanoseconds (same as heap, no GC)
        Size: limited by physical RAM

Tier 2: Memory-Mapped Files (evicted snapshots)
        Access: microseconds (page fault on first access)
        Size: limited by local disk

Tier 3: Distributed File System (archived snapshots, HDFS/S3)
        Access: milliseconds
        Size: unlimited
```

```java
public interface EvictionPolicy {
    /** Decide which snapshots to evict from the current tier. */
    List<SnapshotId> selectForEviction(
        Map<SnapshotId, EvictionMetadata> candidates,
        long currentUsage, long maxUsage);
}

public record EvictionMetadata(
    Instant lastAccessed,
    long sizeBytes,
    int accessCount,
    int sharedNodeCount  // nodes shared with other versions
) {}
```

---

## 12. GAS Engine & Built-in Algorithms (tegra-algorithms)

### 12.1 PageRank

```java
public final class PageRank<E> implements VertexProgram<Double, E, Double> {

    private final double dampingFactor;
    private final double tolerance;
    private final int maxIterations;

    @Override
    public EdgeDirection gatherDirection() { return EdgeDirection.IN; }

    @Override
    public EdgeDirection scatterDirection() { return EdgeDirection.OUT; }

    @Override
    public Double gather(Double vertexValue, E edgeValue, Double neighborValue) {
        // Neighbor contributes its rank divided by its out-degree
        return neighborValue; // out-degree normalization done in apply
    }

    @Override
    public Double sum(Double a, Double b) { return a + b; }

    @Override
    public Double apply(Double currentValue, Double gathered) {
        return (1.0 - dampingFactor) + dampingFactor * gathered;
    }

    @Override
    public boolean scatter(Double updatedValue, Double oldValue, E edgeValue) {
        return Math.abs(updatedValue - oldValue) > tolerance;
    }

    @Override
    public Double identity() { return 0.0; }

    @Override
    public boolean hasConverged(Double oldValue, Double newValue) {
        return Math.abs(oldValue - newValue) < tolerance;
    }
}
```

### 12.2 Connected Components (Label Propagation)

```java
public final class ConnectedComponents<E>
        implements VertexProgram<Long, E, Long> {

    @Override
    public EdgeDirection gatherDirection() { return EdgeDirection.BOTH; }

    @Override
    public EdgeDirection scatterDirection() { return EdgeDirection.BOTH; }

    @Override
    public Long gather(Long vertexValue, E edgeValue, Long neighborValue) {
        return neighborValue;
    }

    @Override
    public Long sum(Long a, Long b) { return Math.min(a, b); }

    @Override
    public Long apply(Long currentValue, Long gathered) {
        return Math.min(currentValue, gathered);
    }

    @Override
    public boolean scatter(Long updatedValue, Long oldValue, E edgeValue) {
        return !updatedValue.equals(oldValue);
    }

    @Override
    public Long identity() { return Long.MAX_VALUE; }
}
```

### 12.3 Additional Algorithms

| Algorithm | Gather | Apply | Scatter | Notes |
|-----------|--------|-------|---------|-------|
| **BFS** | Min distance from neighbors | Min(current, gathered + 1) | Changed? | Single-source |
| **SSSP** | Min (neighbor dist + edge weight) | Min(current, gathered) | Changed? | Dijkstra-like |
| **TriangleCount** | Count common neighbors | Store count | Always | Set intersection |
| **BeliefPropagation** | Product of incoming messages | Normalize | Always | Loopy BP |
| **LabelPropagation** | Mode of neighbor labels | Adopt majority | Changed? | Community detection |
| **k-Hop** | Union of neighbor hop sets | Add current vertex | Hops < k? | k-neighborhood |
| **CollabFiltering** | Weighted neighbor ratings | Update latent factors | Always | ALS-based |

---

## 13. Memory Management Strategy

### 13.1 Overview

The GC concern is addressed by a **hybrid memory model**:

```
┌─────────────────────────────────────────────────────────────┐
│                    Java Heap (ZGC managed)                   │
│                                                             │
│  ┌──────────────┐  ┌────────────┐  ┌─────────────────────┐ │
│  │ VersionMap   │  │ API objects│  │ Computation state   │ │
│  │ (small HAMT) │  │ (records)  │  │ (vertex values,     │ │
│  │              │  │            │  │  messages, etc.)     │ │
│  └──────────────┘  └────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│               Off-Heap Direct Memory (Arena managed)         │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ ART/HAMT Tree Nodes                                  │   │
│  │ (the bulk of memory: 80%+ of total usage)            │   │
│  │                                                      │   │
│  │ Reference counted. Freed when no version references  │   │
│  │ the node. Not visible to GC. Zero pause overhead.    │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│           Memory-Mapped Files (OS page cache managed)        │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Evicted snapshot subtrees                            │   │
│  │ .tsnap files on local SSD                            │   │
│  │ Paged in on-demand by OS; paged out under pressure   │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 13.2 Reference Counting for Off-Heap Nodes

Since off-heap memory is not GC-managed, we use reference counting:

```java
public abstract class RefCounted {
    private final AtomicInteger refCount = new AtomicInteger(1);
    private final OffHeapArena arena;
    private final long offset;

    public void retain() {
        refCount.incrementAndGet();
    }

    public void release() {
        if (refCount.decrementAndGet() == 0) {
            arena.free(offset, size());
        }
    }
}
```

When a snapshot is created via path-copying:
- The **new** nodes (on the copied path) start with refCount = 1
- The **shared** nodes (not on the path) get `retain()` called (refCount increases)

When a snapshot is evicted:
- Walk the tree from its root, calling `release()` on each node
- Nodes with refCount > 1 are shared with other versions — they survive
- Nodes reaching refCount = 0 are immediately freed

### 13.3 Batch Commit Arena

During a `commit()` with many mutations, we use a dedicated arena for path-copied nodes:

```java
public SnapshotId commit(MutableGraphView<V, E> working) {
    try (var commitArena = new OffHeapArena(estimatedCommitSize())) {
        // All path-copied nodes go into this arena
        // Arena provides sequential allocation (cache-friendly)
        // Arena tracks all allocations for bulk reference management
        var newRoot = working.finalize(commitArena);
        versions.put(id, newRoot);
    }
    // commitArena.close() does NOT free memory —
    // it transfers ownership to the version map
}
```

### 13.4 JVM Tuning Recommendations

```bash
# Recommended JVM flags for Tegra-J
java \
  -XX:+UseZGC \                          # Sub-ms pauses
  -XX:+ZGenerational \                   # Generational ZGC (Java 21+)
  -Xmx8g \                              # Modest heap (most data is off-heap)
  -XX:MaxDirectMemorySize=64g \          # Off-heap budget for tree nodes
  --enable-preview \                     # For structured concurrency, scoped values
  --add-modules jdk.incubator.vector \   # For SIMD in ART Node16 key search
  -jar tegra-j.jar
```

---

## 14. Java 21 Feature Utilization Map

| Java 21 Feature | Where Used | How |
|-----------------|-----------|-----|
| **Records (JEP 395)** | `SnapshotId`, `VertexId`, `EdgeId`, `GraphDelta`, `DiffEntry`, `VersionRoot`, `PathCopyResult`, `EvictionMetadata` | Immutable value types with structural equality. Reduces boilerplate by ~60% |
| **Sealed Interfaces (JEP 409)** | `ArtNode`, `HamtNode`, `GraphView`, `Partitioner`, `ChangeType` | Exhaustive type hierarchies enabling pattern-match dispatch without `instanceof` chains |
| **Pattern Matching for switch (JEP 441)** | Tree traversal in `PersistentART`, `PersistentHAMT`, diff algorithm, serialization | `case Node4 n4 -> ...` replaces visitor pattern. JIT can optimize as a tableswitch |
| **Virtual Threads (JEP 444)** | RPC handlers, distributed GAS phases, parallel snapshot operations, Timelapse.map() | One virtual thread per partition per superstep. Millions concurrent. No thread pool tuning |
| **Structured Concurrency (JEP 462)** | Distributed commit barrier, parallel diff computation, Timelapse.map() | Fork-join with cancellation. Failure in one partition cancels all |
| **Scoped Values (JEP 446)** | Snapshot context in GAS computation, current MutationContext, trace IDs | Thread-local-like but immutable and virtual-thread-friendly |
| **Foreign Memory API (JEP 454)** | Off-heap tree nodes, memory-mapped snapshot files, zero-copy serialization | `Arena`, `MemorySegment`, `MemoryLayout` for tree node allocation |
| **Vector API (JEP 448)** | ART Node16 key search (SIMD comparison), batch hash computation | 16-byte key comparison in one instruction on x86 AVX2 |
| **Sequenced Collections (JEP 431)** | Timelapse snapshot ordering, iteration state sequences | `SequencedMap` for version map with first/last access |

---

## 15. API Design — The Public Contract

### 15.1 Builder Pattern Entry Point

```java
// Create a new timelapse for a graph
Timelapse<UserProfile, FollowEdge> twitter = Timelapse.builder("twitter")
    .vertexType(UserProfile.class)
    .edgeType(FollowEdge.class)
    .storageDir(Path.of("/data/tegra/twitter"))
    .maxInMemorySnapshots(500)
    .evictionPolicy(EvictionPolicy.lru())
    .build();

// Load initial graph
twitter.ingest(GraphLoader.fromEdgeList(Path.of("twitter.edges")));
twitter.save("TWTR_1577869200");

// Apply mutations and create new snapshot
var working = twitter.branch("TWTR_1577869200");
working.addEdge(42L, 99L, new FollowEdge(Instant.now()));
working.removeEdge(13L, 7L);
twitter.save(working, "TWTR_1577872800");

// Ad-hoc query on any snapshot
double[] ranks = twitter.run("TWTR_1577869200",
    new PageRank<>(0.85, 1e-6, 20));

// Incremental query across multiple snapshots
var snapshots = twitter.range("TWTR_1577869200", "TWTR_1577872800");
Map<SnapshotId, double[]> allRanks = twitter.mapIncremental(
    snapshots.stream().map(GraphSnapshot::id).toList(),
    new PageRank<>(0.85, 1e-6, 20));

// What-if analysis
var whatIf = twitter.branch("TWTR_1577869200");
whatIf.removeVertex(42L);  // What if user 42 left?
twitter.save(whatIf, "TWTR_WHATIF_NO42");
double[] altRanks = twitter.run("TWTR_WHATIF_NO42",
    new PageRank<>(0.85, 1e-6, 20));

// Compare
GraphDelta<UserProfile, FollowEdge> delta =
    twitter.diff("TWTR_1577869200", "TWTR_1577872800");
System.out.println("Changed vertices: " + delta.changedVertexIds().size());
```

### 15.2 Algorithm SPI

Users can implement custom algorithms:

```java
public final class MyCustomAlgorithm
    implements VertexProgram<MyVertexValue, MyEdgeValue, MyMessage> {

    // Implement gather, sum, apply, scatter...
}

// Register via ServiceLoader or direct instantiation
var result = timelapse.run(snapshotId, new MyCustomAlgorithm());
```

### 15.3 Fluent Query API

```java
// Temporal comparison
var comparison = timelapse.compare()
    .snapshot("TWTR_1577869200")
    .snapshot("TWTR_1577872800")
    .using(new ConnectedComponents<>())
    .execute();

int componentsBefore = comparison.get("TWTR_1577869200").componentCount();
int componentsAfter = comparison.get("TWTR_1577872800").componentCount();

// Sliding window
var results = timelapse.window()
    .from("TWTR_1577869200")
    .to("TWTR_1577908800")
    .stride(Duration.ofHours(1))
    .algorithm(new PageRank<>(0.85, 1e-6, 20))
    .execute();
```

---

## 16. Integration Points & Ecosystem

### 16.1 Apache TinkerPop Compatibility

Tegra-J can implement the TinkerPop `Graph` interface, enabling Gremlin query language support:

```java
// tegra-tinkerpop module (optional)
public final class TegraGraph implements Graph {
    private final Timelapse<?, ?> timelapse;
    private final SnapshotId activeSnapshot;

    @Override
    public Vertex addVertex(Object... keyValues) { /* ... */ }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) { /* ... */ }

    // Gremlin traversals work on any snapshot
    public TegraGraph atSnapshot(SnapshotId id) { /* ... */ }
}
```

### 16.2 Apache Arrow Integration

For interop with Python (pandas, NetworkX) and analytics tools:

```java
// Export a snapshot as Arrow RecordBatches
var vertexBatch = ArrowBridge.verticesToArrow(snapshot, allocator);
var edgeBatch = ArrowBridge.edgesToArrow(snapshot, allocator);

// Zero-copy sharing via Arrow IPC
ArrowFileWriter.write(vertexBatch, channel);
```

### 16.3 Apache Kafka / Flink Ingestion

```java
// Ingest graph mutations from Kafka
var ingester = KafkaGraphIngester.builder()
    .topic("graph-mutations")
    .bootstrapServers("kafka:9092")
    .timelapse(timelapse)
    .batchSize(10_000)         // Commit snapshot every 10K mutations
    .build();

ingester.start(); // Runs on virtual threads
```

### 16.4 REST API (Optional tegra-server module)

```
GET  /api/v1/timelapse/{graphId}/snapshots
GET  /api/v1/timelapse/{graphId}/snapshots/{snapshotId}
POST /api/v1/timelapse/{graphId}/snapshots/{snapshotId}/query
     Body: {"algorithm": "pagerank", "params": {"damping": 0.85}}
GET  /api/v1/timelapse/{graphId}/diff/{snapA}/{snapB}
POST /api/v1/timelapse/{graphId}/branch/{snapshotId}
```

---

## 17. Testing Strategy

### 17.1 Test Pyramid

```
                    ┌───────────────┐
                    │   End-to-End  │  tegra-benchmark (JMH)
                    │   Benchmarks  │  Real datasets (Twitter, UK)
                    └───────┬───────┘
                   ┌────────┴────────┐
                   │  Integration    │  Multi-module, distributed commit,
                   │  Tests          │  ICE end-to-end, eviction round-trip
                   └────────┬────────┘
              ┌─────────────┴─────────────┐
              │  Component Tests           │  Per-module: DGSI ops, GAS engine,
              │                            │  diff correctness, serialization
              └─────────────┬─────────────┘
         ┌──────────────────┴──────────────────┐
         │  Unit Tests (Property-Based)         │  HAMT/ART invariants, structural
         │                                      │  sharing verification, diff symmetry
         └──────────────────────────────────────┘
```

### 17.2 Critical Correctness Tests

1. **Structural sharing verification**: After path-copying, count shared vs. unique nodes. Verify that shared fraction matches theoretical expectation.

2. **ICE correctness**: For every incremental computation, also run full computation. Assert bit-identical results. This directly verifies the paper's claim that "ICE generates the exact same intermediate states for all edges and vertices at all iterations" (Section 4.2).

3. **Snapshot isolation**: Mutate a branched snapshot. Verify original snapshot is unchanged. Concurrent reads during mutation must see consistent state.

4. **Diff symmetry**: `diff(A, B)` and `diff(B, A)` should produce inverse deltas.

5. **Eviction round-trip**: Evict a snapshot to disk. Reload it. Verify bit-identical to the in-memory version. Run the same algorithm — verify identical results.

6. **Distributed commit consistency**: All partitions must have the same logical snapshot after a barrier commit. Inject failures (partition timeout, network partition) and verify recovery.

### 17.3 Property-Based Testing (jqwik)

```java
@Property
void hamtStructuralSharing(
    @ForAll @Size(min = 100, max = 10000) List<@IntRange(min = 0, max = 100000) Integer> keys,
    @ForAll @IntRange(min = 0, max = 100) int mutationPercent
) {
    var hamt1 = PersistentHAMT.<Integer, String>empty();
    for (int k : keys) hamt1 = hamt1.put(k, "v" + k);

    int mutations = keys.size() * mutationPercent / 100;
    var hamt2 = hamt1;
    for (int i = 0; i < mutations; i++) {
        hamt2 = hamt2.put(keys.get(i), "modified");
    }

    // Verify: shared node count >= (1 - mutationPercent/100) * totalNodes
    double sharedFraction = countSharedNodes(hamt1, hamt2) / (double) countNodes(hamt1);
    assertThat(sharedFraction).isGreaterThan(1.0 - (mutationPercent / 100.0) - 0.1);
}

@Property
void iceCorrectnessMatchesFullExecution(
    @ForAll("smallGraphs") GraphSnapshot<Long, Void> graph,
    @ForAll @IntRange(min = 1, max = 100) int edgeMutations
) {
    // Full execution
    var fullResult = gasEngine.execute(graph, new ConnectedComponents<>(), 100, null);

    // Incremental execution
    var mutated = applyRandomMutations(graph, edgeMutations);
    var iceResult = iceEngine.executeIncremental(mutated,
        new ConnectedComponents<>(), previousStates, SwitchOracle.never());

    // Must be identical
    assertGraphValuesEqual(fullResult, iceResult);
}
```

---

## 18. Benchmarking & Evaluation Plan

### 18.1 Microbenchmarks (JMH)

| Benchmark | What it measures | Target |
|-----------|-----------------|--------|
| HAMT.put | Single-key insertion throughput | > 5M ops/sec |
| HAMT.get | Single-key lookup throughput | > 20M ops/sec |
| HAMT.pathCopy | Path-copy depth and allocation | O(7) nodes for 1B entries |
| HAMT.diff | Diff throughput (two 1M-entry tries, 1% changed) | < 10ms |
| ART.put/get | Same as HAMT but for ART | ART should be 2-3x faster on range scans |
| SnapshotCommit | End-to-end commit with 10K mutations | < 100ms |
| SnapshotRetrieve | Retrieval of a random snapshot from 1000 stored | < 2s (match paper) |
| GAS.pagerank | Single iteration of PageRank on 1M vertex graph | < 500ms |
| ICE.incremental | Incremental PR on 0.1% changed graph | < 50ms |

### 18.2 Macro Benchmarks (Reproduce Paper Results)

Following the paper's evaluation methodology (Section 7):

1. **Dataset**: Twitter (41.6M V, 1.47B E), UK-2007 (105.9M V, 3.74B E)
   - Available from SNAP and WebGraph
   - Simulate evolution: start with 80% edges, add 1% per snapshot

2. **Snapshot retrieval latency** (Table 3): Store 200-1000 snapshots. Measure average of 10 random retrievals. Compare against our baseline (full materialization).

3. **Computation state overhead** (Figure 7): Run PR and CC incrementally, measure memory after every 200 computations up to 1000.

4. **Ad-hoc window operations** (Figures 8-9): 100 random windows, 0.1% change rate. Measure single-snapshot and 10-snapshot window query times.

5. **Timelapse parallel speedup** (Figure 10): CC on 1-20 snapshots. Measure total time vs. serial baseline.

6. **ICE switching** (Figure 11): CC with targeted high-impact deletions. Compare with/without switch oracle.

### 18.3 Comparison Baselines

For an OSS project, compare against:
- **JGraphT** (single-node, no versioning, no incremental — strawman baseline)
- **Apache Giraph** (distributed, no versioning)
- **Neo4j** (property graph store, no computational model)
- **Naive copy-on-write** (full graph copy per snapshot)

---

## 19. Phased Implementation Roadmap

### Phase 0: Project Bootstrap (Week 1)

- [ ] Maven multi-module project setup with JPMS
- [ ] CI/CD pipeline (GitHub Actions: build, test, benchmark)
- [ ] Code style (Google Java Format), static analysis (Error Prone, SpotBugs)
- [ ] CONTRIBUTING.md, CODE_OF_CONDUCT.md, LICENSE (Apache 2.0)
- [ ] Benchmarking infrastructure (JMH, Gradle benchmark plugin)

### Phase 1: Persistent Data Structures (Weeks 2-4)

- [ ] `PersistentHAMT` with path-copying and transient mutation optimization
- [ ] Off-heap `OffHeapArena` and `NodeAllocator`
- [ ] Reference counting for off-heap nodes
- [ ] `DiffIterator` for efficient structural diff
- [ ] Property-based test suite (jqwik)
- [ ] JMH benchmarks for HAMT operations
- **Milestone**: HAMT passes all correctness tests, demonstrates structural sharing with sublinear memory growth over 1000 versions

### Phase 2: Single-Node DGSI (Weeks 4-6)

- [ ] `GraphPartition` with vertex and edge trees
- [ ] `VersionMap` with snapshot lifecycle management
- [ ] `commit()`, `get()`, `branch()`, `diff()` operations
- [ ] `MutationLog` for inter-snapshot mutation tracking
- [ ] Basic LRU `EvictionManager`
- **Milestone**: Can store 100 snapshots of a 1M-vertex graph, retrieve any in < 1s

### Phase 3: Timelapse API (Weeks 6-7)

- [ ] `Timelapse<V,E>` with full API surface
- [ ] `GraphSnapshot<V,E>` immutable views
- [ ] `MutableGraphView<V,E>` for branching
- [ ] `SnapshotId` with prefix/range matching
- [ ] `TimelapseBuilder` with configuration
- **Milestone**: Users can create, mutate, commit, branch, and retrieve snapshots via the public API

### Phase 4: GAS Engine (Weeks 7-9)

- [ ] `VertexProgram<V,E,M>` interface
- [ ] `GasEngine` with gather/apply/scatter loop
- [ ] `PageRank`, `ConnectedComponents`, `BFS` implementations
- [ ] Iteration state saving to Timelapse
- **Milestone**: PageRank runs correctly on a 1M-vertex graph in < 5s for 20 iterations

### Phase 5: ICE Engine (Weeks 9-12)

- [ ] `DiffEngine` using persistent tree referential equality
- [ ] `NeighborhoodExpander` for 1-hop expansion
- [ ] `IceEngine` with bootstrap, iteration, termination phases
- [ ] `StateMerger` for copying unchanged state
- [ ] `SwitchOracle` (heuristic-based default)
- [ ] ICE correctness test suite (every incremental result verified against full execution)
- **Milestone**: ICE achieves 5-10x speedup on 0.1% mutation workloads vs. full recomputation

### Phase 6: Serialization & Eviction (Weeks 12-14)

- [ ] Binary `.tsnap` format for subtree serialization
- [ ] `MMapSnapshotLoader` for memory-mapped reload
- [ ] Full tiered eviction pipeline (heap → off-heap → disk)
- [ ] Eviction round-trip correctness tests
- **Milestone**: System can store 1000+ snapshots with graceful memory management

### Phase 7: Distribution Layer (Weeks 14-18)

- [ ] `HashPartitioner` for vertex partitioning
- [ ] `VirtualThreadDispatcher` for RPC
- [ ] `BarrierCoordinator` for distributed commits
- [ ] Distributed GAS with cross-partition message passing
- [ ] Distributed ICE with cross-partition neighborhood expansion
- **Milestone**: 4-node cluster runs distributed PageRank on partitioned Twitter graph

### Phase 8: Persistent ART (Weeks 18-21)

- [ ] `PersistentART` with Node4/Node16/Node48/Node256
- [ ] Prefix compression, adaptive node growth/shrink
- [ ] ART-specific diff optimization
- [ ] Benchmark: ART vs. HAMT for graph workloads
- **Milestone**: ART provides 2-3x improvement over HAMT for range-scan-heavy workloads

### Phase 9: Optimizations & Polish (Weeks 21-24)

- [ ] Parallel ICE across multiple snapshots
- [ ] Sliding window query optimization
- [ ] Apache Arrow bridge
- [ ] TinkerPop compatibility layer
- [ ] Comprehensive documentation and examples
- [ ] Performance regression test suite

### Phase 10: OSS Launch (Week 24+)

- [ ] Final API review and stabilization
- [ ] Performance report (reproduce paper results)
- [ ] Blog post, conference talk
- [ ] Apache incubator proposal (if pursuing ASF route)

---

## 20. OSS Project Governance & Community

### 20.1 Licensing

**Apache License 2.0** — the standard for data infrastructure projects. Allows commercial use, modification, and distribution. Patent grant protects users.

### 20.2 Project Structure (Apache-Style)

```
Community Roles:
  PMC Chair      — overall project stewardship
  PMC Members    — binding votes on releases, new committers
  Committers     — write access to repository
  Contributors   — anyone with a merged PR

Decision Making:
  - Lazy consensus for minor changes
  - 3 binding +1 votes for releases
  - Majority PMC vote for new committers
  - DISCUSS threads for significant design changes
```

### 20.3 Documentation Strategy

- **User Guide**: Getting started, API reference, configuration, deployment
- **Developer Guide**: Architecture overview, how to add algorithms, how to implement custom partitioners
- **Javadoc**: All public API classes with examples
- **Design Documents**: ADRs (Architecture Decision Records) for major choices
- **Benchmarks**: Reproducible benchmark suite with published results

### 20.4 Release Strategy

- **Semantic versioning**: MAJOR.MINOR.PATCH
- **0.x releases**: API unstable, rapid iteration, community feedback
- **1.0 release**: API stable, backward compatibility guaranteed
- **Release cadence**: Monthly during 0.x, quarterly after 1.0

### 20.5 CI/CD Pipeline

```yaml
# GitHub Actions
on: [push, pull_request]
jobs:
  build:
    matrix:
      java: [21, 22, 23]      # Test multiple JDK versions
      os: [ubuntu, macos]
    steps:
      - mvn verify              # Unit + integration tests
      - mvn spotbugs:check      # Static analysis
      - mvn javadoc:javadoc     # Verify Javadoc compiles

  benchmark:
    if: github.ref == 'refs/heads/main'
    steps:
      - mvn -pl tegra-benchmark jmh:benchmark
      - publish results to GitHub Pages

  compatibility:
    steps:
      - test with ZGC, Shenandoah, G1
      - test with GraalVM native-image (where applicable)
```

---

## 21. Risk Analysis & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **Off-heap memory leaks** (reference counting errors) | Medium | High | Rigorous leak detection in tests; `OffHeapArena` tracks all allocations; periodic integrity checks in debug mode |
| **GC pressure from vertex properties** (heap-side data) | Medium | Medium | Use primitive specializations (`DoubleVertexStore`, `LongVertexStore`) to avoid boxing. ZGC sub-ms pauses for remainder |
| **Virtual thread pinning** (synchronized blocks in hot path) | Low | Medium | Use `ReentrantLock` instead of `synchronized` in all concurrent code. Monitor with JFR pinning events |
| **ART/HAMT performance vs. native** | Medium | Medium | HAMT-first strategy de-risks. If ART perf insufficient, integrate via JNI/Panama FFI to a C library |
| **Distributed coordination complexity** | High | High | Start single-node. Distribution is opt-in. Simple barrier protocol. Don't try to be Spark |
| **API instability delaying adoption** | Medium | High | 0.x period with explicit "experimental" labels. Gather feedback before 1.0. Minimalist public API surface |
| **Scope creep** (trying to also be a streaming engine) | Medium | Medium | Explicit non-goal. Tegra-J is for ad-hoc analytics. Streaming integration via adapters only |
| **Competitive landscape** (Neo4j, TigerGraph, etc.)** | Medium | Low | Different niche. No existing system does ad-hoc temporal graph analytics with computation reuse |

---

## Appendix A: Key Data Structure Pseudocode

### A.1 HAMT Insert with Path-Copying

```
function insert(node, key, value, hash, shift, ctx):
    if node is EmptyNode:
        return new Leaf(key, value)

    if node is Leaf:
        if node.key == key:
            return new Leaf(key, value)  // Replace
        else:
            return splitLeaf(node, key, value, hash, shift)

    if node is BitmapIndexedNode:
        bit = bitpos(hash, shift)       // 1 << ((hash >>> shift) & 0x1f)
        idx = bitcount(bitmap & (bit - 1))

        if (bitmap & bit) == 0:
            // No child at this position — add one
            newArray = insertAt(node.contents, idx, key, value)
            newBitmap = bitmap | bit
            if editable(node, ctx):
                node.bitmap = newBitmap
                node.contents = newArray
                return node             // In-place mutation (transient)
            else:
                return new BitmapIndexedNode(newBitmap, newArray)  // Path-copy
        else:
            // Child exists — recurse
            child = node.contents[idx]
            newChild = insert(child, key, value, hash, shift + 5, ctx)
            if newChild == child:
                return node             // No change
            newArray = replaceAt(node.contents, idx, newChild)
            if editable(node, ctx):
                node.contents = newArray
                return node
            else:
                return new BitmapIndexedNode(bitmap, newArray)
```

### A.2 Persistent ART Insert with Path-Copying

```
function artInsert(node, key, depth, value, ctx):
    if node is null:
        return new ArtLeaf(key, value)

    if node is ArtLeaf:
        if node.key == key:
            return new ArtLeaf(key, value)
        // Create new inner node with two leaves
        newNode = new Node4()
        commonPrefix = longestCommonPrefix(node.key, key, depth)
        newNode.prefix = commonPrefix
        newNode.addChild(key[depth + commonPrefix.length], new ArtLeaf(key, value))
        newNode.addChild(node.key[depth + commonPrefix.length], node)
        return newNode

    if node is InnerNode (Node4/16/48/256):
        // Check prefix
        mismatch = prefixMismatch(node.prefix, key, depth)
        if mismatch < node.prefixLen:
            // Split prefix
            newNode = new Node4()
            newNode.prefix = key[depth..depth+mismatch]
            newNode.addChild(node.prefix[mismatch], node.shrinkPrefix(mismatch+1))
            newNode.addChild(key[depth+mismatch], new ArtLeaf(key, value))
            return newNode

        depth += node.prefixLen
        child = node.findChild(key[depth])
        if child is not null:
            newChild = artInsert(child, key, depth + 1, value, ctx)
            return pathCopy(node, key[depth], newChild, ctx)  // Copy this node, replace child
        else:
            return pathCopy(node, key[depth], new ArtLeaf(key, value), ctx)  // Copy and add child
```

---

## Appendix B: API Surface Draft

### B.1 Core Types (tegra-api)

```java
// === Value types (records) ===
record SnapshotId(byte[] raw) implements Comparable<SnapshotId> {}
record VertexId(long id) {}
record EdgeId(long src, long dst, short discriminator) {}
record Vertex<V>(long id, V properties) {}
record Edge<E>(long src, long dst, E properties) {}
record GraphDelta<V, E>(
    List<DiffEntry<Long, V>> vertexChanges,
    List<DiffEntry<EdgeId, E>> edgeChanges) {}
record DiffEntry<K, V>(K key, V oldValue, V newValue, ChangeType type) {}

// === Core interfaces ===
sealed interface GraphView<V, E> permits GraphSnapshot, MutableGraphView {}
final class GraphSnapshot<V, E> implements GraphView<V, E> {}  // immutable
final class MutableGraphView<V, E> implements GraphView<V, E> {}  // mutable

// === Main entry point ===
final class Timelapse<V, E> {
    static <V, E> TimelapseBuilder<V, E> builder(String graphId);
    SnapshotId save(String id);
    SnapshotId save(MutableGraphView<V, E> working, String id);
    GraphSnapshot<V, E> retrieve(SnapshotId id);
    List<GraphSnapshot<V, E>> retrieve(String prefix);
    List<GraphSnapshot<V, E>> range(SnapshotId from, SnapshotId to);
    GraphDelta<V, E> diff(SnapshotId a, SnapshotId b);
    MutableGraphView<V, E> branch(SnapshotId source);
    <R> R run(SnapshotId id, GraphAlgorithm<V, E, R> algorithm);
    <R> Map<SnapshotId, R> map(List<SnapshotId> ids, GraphAlgorithm<V, E, R> algo);
    <R> Map<SnapshotId, R> mapIncremental(List<SnapshotId> ids, GraphAlgorithm<V, E, R> algo);
    <R> List<R> window(SnapshotId start, SnapshotId end, Duration stride, GraphAlgorithm<V, E, R> algo);
}

// === Computation ===
interface VertexProgram<V, E, M> {
    EdgeDirection gatherDirection();
    EdgeDirection scatterDirection();
    M gather(V vertexValue, E edgeValue, V neighborValue);
    M sum(M a, M b);
    V apply(V currentValue, M gathered);
    boolean scatter(V updatedValue, V oldValue, E edgeValue);
    M identity();
}

// === SPI extension points ===
interface EvictionPolicy {}
interface Partitioner {}
interface Serializer {}
interface ComputeEngine {}
interface SwitchOracle {}
```

### B.2 Module Exports

| Module | Exports | Opens |
|--------|---------|-------|
| `org.tegra.api` | `org.tegra.api`, `org.tegra.api.config` | — |
| `org.tegra.pds` | `org.tegra.pds.art`, `org.tegra.pds.hamt`, `org.tegra.pds.common` | — |
| `org.tegra.store` | `org.tegra.store` (to `org.tegra.api` only) | — |
| `org.tegra.compute` | `org.tegra.compute.gas`, `org.tegra.compute.spi` | — |
| `org.tegra.algorithms` | `org.tegra.algorithms` | — |
| `org.tegra.serde` | `org.tegra.serde` (to `org.tegra.store` only) | — |
| `org.tegra.cluster` | `org.tegra.cluster` | — |

---

## Appendix C: Comparison with Existing Java Graph Libraries

| Feature | **Tegra-J** | JGraphT | Apache Giraph | Neo4j | TinkerPop |
|---------|-----------|---------|---------------|-------|-----------|
| **Temporal versioning** | Native (DGSI) | No | No | No (manual snapshots) | No |
| **Structural sharing** | Persistent ART/HAMT | No | No | No | No |
| **Incremental computation** | ICE | No | No | No | No |
| **Ad-hoc window queries** | Native | No | No | Manual | No |
| **Property graph** | Yes | Yes | Yes | Yes | Yes |
| **Distributed** | Yes (opt-in) | No | Yes (Hadoop) | Cluster (enterprise) | Varies |
| **GAS model** | Native | No | Yes | No | Via OLAP |
| **In-memory** | Yes (off-heap) | Yes (heap) | No (disk-based) | Hybrid | Varies |
| **Algorithm library** | Growing | Extensive | Limited | Procedures | Varies |
| **License** | Apache 2.0 | LGPL/EPL | Apache 2.0 | GPL/Commercial | Apache 2.0 |

**Key differentiator**: No existing Java library combines temporal versioning, structural sharing, and incremental computation. Tegra-J occupies a unique niche.

---

## Appendix D: Glossary

| Term | Definition |
|------|-----------|
| **Persistent Data Structure** | A data structure that preserves previous versions when modified; modifications create new versions sharing structure with old ones |
| **Path-Copying** | The technique of creating a new version by copying only the nodes on the path from root to the modified leaf |
| **Structural Sharing** | The property that two versions of a persistent data structure share unchanged subtrees |
| **HAMT** | Hash Array Mapped Trie — a persistent hash table using bitmap-indexed trie nodes |
| **ART** | Adaptive Radix Tree — a radix tree with nodes that adapt their size (4, 16, 48, 256 children) based on density |
| **GAS** | Gather-Apply-Scatter — a vertex-centric programming model for graph-parallel computation |
| **ICE** | Incremental Computation by Entity Expansion — Tegra's technique for avoiding redundant computation |
| **DGSI** | Distributed Graph Snapshot Index — Tegra's versioned graph store |
| **Timelapse** | The user-facing abstraction: a sequence of immutable graph snapshots |
| **Snapshot** | An immutable, point-in-time view of the entire graph |
| **Virtual Thread** | A lightweight thread managed by the JVM, not the OS; can block without consuming an OS thread |
| **Arena** | A memory allocation scope from Java's Foreign Memory API; manages off-heap memory lifecycle |

---

*This research document was produced on 2026-03-14 based on analysis of the Tegra NSDI 2021 paper and implementation guide. It represents a comprehensive blueprint for building Tegra-J as a production-quality, Apache-grade open-source project in Java 21.*
