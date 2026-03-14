# Tegra-J: Implementation Research Document

**Paper**: TEGRA: Efficient Ad-Hoc Analytics on Evolving Graphs
**Authors**: Anand Padmanabha Iyer, Qifan Pu, Kishan Patel, Joseph E. Gonzalez, Ion Stoica
**Venue**: NSDI 2021 (18th USENIX Symposium on Networked Systems Design and Implementation)
**Target**: Java 21, Gradle multi-module, Apache-grade OSS

---

## 1. Paper Synopsis

Tegra is a system that enables efficient ad-hoc window operations on time-evolving graphs. It solves the problem of performing ad-hoc queries on arbitrary time windows — past, present, or sliding — without either storing full copies of every snapshot (prohibitive memory) or reconstructing from change logs (prohibitive latency).

The system rests on two key insights about real-world evolving graph workloads:

1. During ad-hoc analysis, graphs change slowly over time relative to their size.
2. Queries are frequently applied to multiple windows relatively close by in time.

Tegra exploits these by combining **persistent data structures** (structural sharing across graph versions for storage efficiency) with **incremental computation** (reusing intermediate results across snapshots for compute efficiency). It achieves up to 30x speedup over state-of-the-art systems for ad-hoc window operation workloads.

### Three Core Components

| Component | Paper Section | Purpose |
|-----------|--------------|---------|
| **Timelapse** | §3 | User-facing abstraction: evolving graph as a sequence of immutable snapshots |
| **ICE** | §4 | Incremental Computation by Entity Expansion: GAS-based incremental model |
| **DGSI** | §5 | Distributed Graph Snapshot Index: versioned graph store using persistent data structures |

### What This Document Covers

This document specifies a **faithful reimplementation** of the Tegra system in Java 21. Every component, API, and behavior described in the paper is mapped to a concrete implementation plan. Nothing is added beyond what the paper specifies. Potential improvements are documented separately in `future-improvements.md`.

---

## 2. System Architecture

The paper defines a layered architecture (§2.4, Figure 2):

```
┌───────────────────────────────────────────────────────────────┐
│                     User Applications                         │
│        (Graph algorithms, ad-hoc queries, what-if analysis)   │
├───────────────────────────────────────────────────────────────┤
│                     Timelapse API (§3)                         │
│    save · retrieve · diff · expand · merge                    │
│    Snapshot-aware graph operators (vertices, mapV, etc.)       │
├───────────────────────────────────────────────────────────────┤
│                     ICE Engine (§4)                            │
│    GAS decomposition · IncPregel · Bootstrap · Iterations     │
│    Learning-based switching (§4.3)                             │
├─────────────────────────┬─────────────────────────────────────┤
│       DGSI (§5)         │       Cluster Layer (§6)            │
│  pART vertex tree       │  Hash partitioning                  │
│  pART edge tree         │  Barrier snapshot protocol          │
│  Version management     │  Direct task communication          │
│  LRU eviction           │  Cross-partition GAS messaging      │
│  Disk serialization     │                                     │
├─────────────────────────┴─────────────────────────────────────┤
│                     Persistent ART (§5.1)                     │
│    Node4 · Node16 · Node48 · Node256 · Leaf                  │
│    Path-copying · Reference counting · Prefix matching        │
└───────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Ingestion**: Updates arrive as batches of vertex/edge mutations. Each update is routed to the correct partition based on vertex hash. After a batch, a coordinated barrier triggers all partitions to `commit` simultaneously, creating a consistent distributed snapshot.

2. **Storage**: Each partition stores its subgraph in two pART trees (vertex, edge). Commits create new tree roots via path-copying. Shared subtrees between versions are reference-counted.

3. **Query**: A user retrieves one or more snapshots via Timelapse. Each snapshot is a pair of pART roots. If previous computation state exists in the timelapse, ICE performs incremental computation. Otherwise, full GAS execution runs.

4. **Eviction**: A background thread monitors snapshot access timestamps. Least-recently-used snapshots have their unique subtrees serialized to disk, replaced by file pointers.

---

## 3. Module Decomposition

```
tegra-j/
├── tegra-pds/            # Persistent Adaptive Radix Tree
├── tegra-store/          # DGSI: versioned graph store
├── tegra-api/            # Timelapse abstraction & user API
├── tegra-compute/        # ICE computation model + GAS engine
├── tegra-cluster/        # Distribution, partitioning, barriers
├── tegra-serde/          # Binary serialization for disk & network
├── tegra-algorithms/     # Standard graph algorithms on GAS
├── tegra-benchmark/      # Evaluation harness (paper §7)
├── tegra-examples/       # Usage examples and demos
├── build.gradle.kts      # Root build configuration
└── settings.gradle.kts   # Module declarations
```

### Module Dependency Graph

```
tegra-pds
    ↑
tegra-serde ──→ tegra-pds
    ↑
tegra-store ──→ tegra-pds, tegra-serde
    ↑
tegra-api ────→ tegra-store
    ↑
tegra-compute → tegra-api, tegra-store
    ↑
tegra-cluster → tegra-compute, tegra-store, tegra-serde
    ↑
tegra-algorithms → tegra-compute, tegra-api
    ↑
tegra-benchmark ─→ tegra-algorithms, tegra-cluster
tegra-examples ──→ tegra-algorithms, tegra-cluster
```

### JPMS Module Names

| Gradle Module | JPMS Module | Exports |
|---------------|-------------|---------|
| `tegra-pds` | `org.tegra.pds` | `org.tegra.pds.art`, `org.tegra.pds.art.node` |
| `tegra-serde` | `org.tegra.serde` | `org.tegra.serde` |
| `tegra-store` | `org.tegra.store` | `org.tegra.store`, `org.tegra.store.graph`, `org.tegra.store.version` |
| `tegra-api` | `org.tegra.api` | `org.tegra.api`, `org.tegra.api.snapshot`, `org.tegra.api.timelapse` |
| `tegra-compute` | `org.tegra.compute` | `org.tegra.compute.gas`, `org.tegra.compute.ice` |
| `tegra-cluster` | `org.tegra.cluster` | `org.tegra.cluster`, `org.tegra.cluster.partition` |
| `tegra-algorithms` | `org.tegra.algorithms` | `org.tegra.algorithms` |
| `tegra-benchmark` | `org.tegra.benchmark` | (none — application module) |

---

## 4. Component Specifications

### 4.1 tegra-pds: Persistent Adaptive Radix Tree (pART)

**Paper reference**: §5.1, §5.2, reference [5] (ankurdave/part), reference [38] (Leis et al., ICDE 2013)

The paper states: *"DGSI uses a persistent version of the Adaptive Radix Tree [38] as its data structure. ART provides several properties useful for graph storage such as efficient updates and range scans. Persistent Adaptive Radix Tree (PART) [5] adds persistence to ART by simple path-copying. For the purpose of building DGSI, we reimplemented PART (hereafter pART) in Scala and made several modifications to optimize it for graph state storage."*

#### 4.1.1 ART Node Types

The Adaptive Radix Tree (Leis et al.) defines four internal node types that adaptively grow/shrink based on the number of children, plus a leaf type:

```java
public sealed interface ArtNode<V>
    permits Node4, Node16, Node48, Node256, Leaf {

    ArtNode<V> insert(byte[] key, int depth, V value);
    ArtNode<V> remove(byte[] key, int depth);
    V lookup(byte[] key, int depth);
    ArtIterator<V> iterator();
    ArtIterator<V> prefixIterator(byte[] prefix);
    int size();
    int refCount();
}
```

| Node Type | Children Capacity | Key Storage | Lookup Strategy |
|-----------|------------------|-------------|-----------------|
| `Node4` | 1–4 | Sorted byte array (4 bytes) | Linear scan |
| `Node16` | 5–16 | Sorted byte array (16 bytes) | Binary search or SIMD-style |
| `Node48` | 17–48 | 256-byte index → 48 child slots | Index lookup |
| `Node256` | 49–256 | Direct 256-slot array | Direct indexing |
| `Leaf` | 0 | Full key stored | Exact match |

Node transitions:

```
Insert: Node4 → Node16 → Node48 → Node256
Remove: Node256 → Node48 → Node16 → Node4
```

#### 4.1.2 Persistence via Path-Copying

The paper specifies path-copying for persistence (§5.1): when a mutation occurs at a leaf, every node on the path from root to that leaf is copied. Unchanged subtrees are shared between old and new versions.

```
insert(key=0x0A_0B_0C, value=v2):

  root₁            root₂ (new)
   |                 |
  [0A]              [0A] (copy)
   |                 |
  [0B]              [0B] (copy)
   |                 |
  leaf(v1)          leaf(v2) (new)

All sibling subtrees of root₁ are shared with root₂.
```

Cost: O(depth) = O(log₂₅₆ n) node copies per mutation. For 64-bit keys, depth ≤ 8.

#### 4.1.3 Path Compression (Lazy Expansion)

ART uses path compression to collapse single-child chains. Two variants exist:

- **Pessimistic**: Store a partial key prefix in each node (bounded length, overflow requires key comparison at leaf).
- **Optimistic**: Store the full partial prefix (variable length, no overflow).

The paper does not specify which variant. We use pessimistic with a maximum prefix length of 8 bytes (matching the original ART paper's recommendation), storing the full key in leaves for disambiguation.

```java
public record PrefixData(byte[] prefix, int length) {}
```

Each internal node carries a `PrefixData` for its compressed path segment.

#### 4.1.4 Reference Counting

The paper states reference counting is used for garbage collection of shared nodes (§5.4): *"Decrement reference counts on all nodes reachable only from this root. Nodes with zero references are freed."*

Each `ArtNode` maintains an `int refCount`. When a new root is created by path-copying, copied nodes start with refCount=1. Shared children have their refCount incremented. When a version is evicted, refCounts are decremented, and nodes reaching zero are freed.

In Java, "freeing" means making the node unreachable for GC. For off-heap nodes (see §5.1), explicit deallocation via `Arena.close()` is used.

#### 4.1.5 Key Types

The paper specifies two key formats (§5.2):

- **Vertex keys**: 64-bit integers. Encoded as 8-byte big-endian arrays for lexicographic ordering in the ART.
- **Edge keys**: Arbitrary byte arrays. Default: `src_id (8 bytes) || dst_id (8 bytes) || discriminator (2 bytes)` = 18 bytes. This enables prefix matching on source vertex to retrieve all outgoing edges.

```java
public final class KeyCodec {
    public static byte[] encodeVertexKey(long vertexId);
    public static long decodeVertexKey(byte[] key);
    public static byte[] encodeEdgeKey(long srcId, long dstId, short discriminator);
    public static EdgeKey decodeEdgeKey(byte[] key);
}

public record EdgeKey(long srcId, long dstId, short discriminator) {}
```

#### 4.1.6 Primitive Specialization

The paper states: *"We create specialized versions of pART to avoid (un)boxing costs when properties are primitive types."*

We provide specialized leaf implementations for common primitive types:

```java
public sealed interface LeafValue
    permits BoxedLeafValue, LongLeafValue, DoubleLeafValue, IntLeafValue {
}

public record LongLeafValue(long value) implements LeafValue {}
public record DoubleLeafValue(double value) implements LeafValue {}
public record IntLeafValue(int value) implements LeafValue {}
public record BoxedLeafValue<V>(V value) implements LeafValue {}
```

#### 4.1.7 Iterators

The paper states: *"providing fast iterators"* as a key engineering optimization. The pART supports:

- **Full iteration**: In-order traversal of all leaves.
- **Prefix iteration**: Traverse only leaves whose keys start with a given prefix. This is critical for Timelapse retrieval (§5.3.1).
- **Range iteration**: Traverse leaves whose keys fall within [start, end). Used for time-windowed snapshot retrieval.

Iterators are implemented as lazy, stack-based traversals to avoid materializing the full key set.

```java
public interface ArtIterator<V> extends Iterator<Map.Entry<byte[], V>> {
    boolean hasNext();
    Map.Entry<byte[], V> next();
    void seekTo(byte[] key);  // jump to first key >= given key
}
```

#### 4.1.8 Bulk Loading Optimization

The paper mentions: *"optimizing path copying under heavy writes."* For bulk ingestion (initial graph load or large batches), we provide a mutable builder that constructs the tree bottom-up without path-copying overhead, then freezes it into a persistent root.

```java
public final class ArtBuilder<V> {
    public void put(byte[] key, V value);
    public ArtNode<V> build();  // returns immutable persistent root
}
```

#### 4.1.9 Memory Layout Considerations

The paper warns about JVM GC overhead: *"Java/Kotlin on JVM would work but the GC overhead may be problematic for a system that creates many short-lived tree nodes during path-copying."*

We mitigate this with three strategies:

1. **Arena-scoped allocation for transient nodes**: During branch-commit operations, path-copied nodes are allocated in a thread-local arena. The paper states: *"Between branch and commit operations, it is likely that many transient child nodes are formed. We aggressively remove them during the commit operation."* Using `Arena.ofConfined()`, transient nodes can be bulk-freed after commit.

2. **In-place updates during branch-commit**: The paper states: *"we enable in-place updates when the operations are local, such as after a branch and before a commit."* When a branch is created, the transient root and its path-copies are mutable until commit. This avoids repeated path-copying for multiple mutations in the same transaction.

3. **Object pooling for internal nodes**: Since ART nodes have a fixed set of sizes (Node4, Node16, Node48, Node256), we pool and reuse node objects to reduce allocation pressure.

---

### 4.2 tegra-store: DGSI (Distributed Graph Snapshot Index)

**Paper reference**: §5

#### 4.2.1 Per-Partition Graph Store

Each partition in DGSI stores (§5.2):

- A **vertex pART**: keys are 64-bit vertex IDs, values are vertex property containers.
- An **edge pART**: keys are byte arrays (src||dst||discriminator), values are edge property containers.
- A **version map**: maps version IDs (byte arrays) to `(vertexRoot, edgeRoot)` pairs.

```java
public final class PartitionStore {
    private final ConcurrentHashMap<ByteArray, VersionEntry> versionMap;
    private final LruEvictionManager evictionManager;
    private final DiskStore diskStore;

    public VersionEntry branch(ByteArray versionId);
    public ByteArray commit(WorkingVersion working, ByteArray newVersionId);
    public GraphView retrieve(ByteArray versionId);
    public void evict(ByteArray versionId);
}

public record VersionEntry(
    ArtNode<VertexData> vertexRoot,
    ArtNode<EdgeData> edgeRoot,
    long lastAccessTimestamp,
    ByteArray logFilePointer    // pointer to mutation log between snapshots
) {}

public record ByteArray(byte[] data) implements Comparable<ByteArray> {
    // Value-based equality, hashCode, compareTo (lexicographic)
}
```

#### 4.2.2 Vertex and Edge Data Model

The paper uses the property graph model (§3): *"Tegra uses the popular property graph model [26], where vertices and edges in the graph are associated with arbitrary properties."*

```java
public record VertexData(
    long id,
    PropertyMap properties
) {}

public record EdgeData(
    long srcId,
    long dstId,
    short discriminator,
    PropertyMap properties
) {}

public final class PropertyMap {
    // Backed by a persistent HAMT or small array for few properties
    // Supports primitive values without boxing via tagged union
    public PropertyMap put(String key, PropertyValue value);
    public PropertyValue get(String key);
    public PropertyMap remove(String key);
    public int size();
    public Iterator<Map.Entry<String, PropertyValue>> iterator();
}

public sealed interface PropertyValue
    permits LongProperty, DoubleProperty, StringProperty, BoolProperty,
            ByteArrayProperty, ListProperty, MapProperty {}
```

The paper notes: *"Tegra creates default properties at vertices and edges to allow queries that compute on them."* Default properties are initialized during graph loading.

#### 4.2.3 Version Management

**Paper reference**: §5.3

The paper specifies two primitives:

**branch(versionId) → WorkingVersion**
- Creates a new transient root pointing to the same children as the source version's root (§5.3).
- The working version is exclusive to the caller — not visible in the system.
- Mutations on the working version use in-place updates (not path-copying) until commit.
- If the source version is on disk, it is first materialized.

**commit(workingVersion, newVersionId) → versionId**
- Freezes the working version by making it persistent.
- Adds the new root pair to the version map.
- Increments reference counts on all nodes reachable from the new roots.
- Aggressively removes transient nodes created during the branch-commit window.
- Stores a pointer to the mutation log (raw mutations between this snapshot and the previous) in the version entry.

```java
public final class WorkingVersion {
    private ArtNode<VertexData> vertexRoot;   // mutable reference
    private ArtNode<EdgeData> edgeRoot;       // mutable reference
    private final ByteArray sourceVersionId;

    public void putVertex(long vertexId, VertexData data);
    public void removeVertex(long vertexId);
    public void putEdge(long srcId, long dstId, short disc, EdgeData data);
    public void removeEdge(long srcId, long dstId, short disc);
    public VertexData getVertex(long vertexId);
    public EdgeData getEdge(long srcId, long dstId, short disc);

    // Iterator over all outgoing edges of a vertex (prefix match on srcId)
    public Iterator<EdgeData> outEdges(long vertexId);
}
```

#### 4.2.4 Version ID Scheme and Matching

**Paper reference**: §5.3.1

Version IDs are byte arrays with a hierarchical naming convention:

```
{GRAPH_ID}_{UNIX_TIMESTAMP}                          → graph snapshot
{GRAPH_ID}_{UNIX_TIMESTAMP}_{ALGO_ID}_{ITERATION}    → computation state
```

Example: `TWTR_1577869200_PR_3` = Twitter graph, timestamp 1577869200, PageRank iteration 3.

DGSI supports matching primitives on version IDs:

- **Prefix matching**: All IDs starting with a prefix. Example: `TWTR_` returns all Twitter snapshots.
- **Suffix matching**: All IDs ending with a suffix.
- **Range matching**: All IDs in a lexicographic range [start, end).

```java
public interface VersionIndex {
    VersionEntry get(ByteArray id);
    List<ByteArray> matchPrefix(ByteArray prefix);
    List<ByteArray> matchSuffix(ByteArray suffix);
    List<ByteArray> matchRange(ByteArray start, ByteArray end);
    void put(ByteArray id, VersionEntry entry);
    void remove(ByteArray id);
}
```

The paper also handles automatic ID generation for time-based snapshots and computation iterations (§5.3.1):

```java
public final class VersionIdGenerator {
    public static ByteArray graphSnapshot(String graphId, long unixTimestamp);
    public static ByteArray computationIteration(
        ByteArray snapshotId, String algorithmId, int iteration);
    public static ByteArray branch(ByteArray sourceId, String branchName);
}
```

#### 4.2.5 GraphView (Read-Only Snapshot Access)

When a version is retrieved, DGSI returns a read-only view backed by the pART roots:

```java
public final class GraphView {
    private final ArtNode<VertexData> vertexRoot;
    private final ArtNode<EdgeData> edgeRoot;
    private final ByteArray versionId;

    public VertexData vertex(long vertexId);
    public EdgeData edge(long srcId, long dstId, short disc);
    public Iterator<VertexData> vertices();
    public Iterator<EdgeData> edges();
    public Iterator<EdgeData> outEdges(long vertexId);    // prefix match
    public Iterator<EdgeData> inEdges(long vertexId);     // requires reverse index or scan
    public long vertexCount();
    public long edgeCount();
}
```

Since snapshots are immutable and backed by persistent data structures, multiple threads can concurrently read different (or the same) snapshots without synchronization. This is a key property enabling parallel computation across snapshots (§3.1, Figure 10).

#### 4.2.6 Memory Management

**Paper reference**: §5.4

**LRU Eviction**:
- Each version access updates a `lastAccessTimestamp` in the version entry.
- A background thread (virtual thread) periodically scans the version map and evicts versions whose `lastAccessTimestamp` falls below a threshold.
- Eviction serializes unique subtrees to disk files. Shared nodes across versions share disk files.
- Evicted nodes are replaced by `DiskPointer` references in the tree.

```java
public final class LruEvictionManager {
    private final long evictionIntervalMs;
    private final long accessThresholdMs;
    private final DiskStore diskStore;
    private final Thread evictionThread;  // virtual thread

    public void start();
    public void stop();
    public void touchVersion(ByteArray versionId);
    void evictVersion(ByteArray versionId);
    void materializeVersion(ByteArray versionId);
}
```

**Disk Storage for Evicted Subtrees**:

```java
public sealed interface ArtNode<V>
    permits Node4, Node16, Node48, Node256, Leaf, DiskNode {
    // DiskNode is a lazy-loading placeholder
}

public final class DiskNode<V> implements ArtNode<V> {
    private final DiskPointer pointer;
    private volatile ArtNode<V> materialized;  // loaded on demand

    // All operations first materialize from disk, then delegate
}

public record DiskPointer(Path filePath, long offset, int length) {}
```

The paper states (§5.4): *"Since every version in DGSI is a branch, we write each subtree in that branch to a separate file and then point its root to the file identifier. By writing subtrees to separate files, we ensure that different versions sharing tree nodes in memory can share tree nodes written to files."*

**Orphan Cleanup**:
The paper states: *"during ad-hoc analysis, analysts are likely to create versions that are never committed. We periodically mark such orphans and adjust the reference counting."*

A background task scans for `WorkingVersion` instances that have not been committed within a configurable timeout and reclaims their resources.

**Reference Counting Mechanics**:

```java
public final class RefCountManager {
    public void increment(ArtNode<?> node);
    public void decrement(ArtNode<?> node);
    public void decrementSubtree(ArtNode<?> root);  // for version eviction
}
```

When a version is evicted via `evict(versionId)`:
1. Remove the root pair from the version map.
2. Walk the tree from the root, decrementing refCounts.
3. For nodes that reach refCount=0 and are not shared: serialize to disk or free.
4. For nodes that are shared (refCount > 0 after decrement): leave in memory.

#### 4.2.7 Mutation Log

The paper states (§5.3): *"In order to be able to retrieve the state of the graph in between snapshots, Tegra stores the updates between snapshots in a simple log file, and adds a pointer to this file to the root."*

```java
public final class MutationLog {
    private final Path logDirectory;

    public MutationLogWriter openWriter(ByteArray versionId);
    public MutationLogReader openReader(ByteArray versionId);
}

public sealed interface GraphMutation
    permits AddVertex, RemoveVertex, UpdateVertexProperty,
            AddEdge, RemoveEdge, UpdateEdgeProperty {}
```

The mutation log enables reconstructing intermediate states between committed snapshots if needed. It is append-only and immutable once the next snapshot is committed.

#### 4.2.8 Graph Import/Export

The paper states (§5.3): *"Tegra can interface with external graph stores, such as Neo4J or Titan for importing and exporting graphs."*

We define an SPI (Service Provider Interface) for graph import/export:

```java
public interface GraphImporter {
    void importGraph(GraphSource source, PartitionStore store, ByteArray versionId);
}

public interface GraphExporter {
    void exportGraph(GraphView view, GraphSink sink);
}

public interface GraphSource {
    Iterator<VertexData> vertices();
    Iterator<EdgeData> edges();
}
```

Built-in importers: edge-list files (TSV/CSV), adjacency list format. External store connectors (Neo4J, etc.) are out of scope for the core system but pluggable via SPI.

---

### 4.3 tegra-api: Timelapse Abstraction & API

**Paper reference**: §3, Table 1

#### 4.3.1 Core Timelapse API

The paper defines five operations in Table 1:

```java
public final class Timelapse {
    private final PartitionStore store;

    /**
     * Save the state of the graph as a snapshot in its timelapse.
     * ID can be autogenerated. Returns the id of the saved snapshot.
     * Internally calls DGSI commit.
     */
    public ByteArray save(ByteArray id);
    public ByteArray save();  // auto-generated ID with timestamp

    /**
     * Return one or more snapshots from the timelapse.
     * Allows simple matching on the id (prefix, range).
     * Internally calls DGSI retrieve with version matching.
     */
    public Snapshot retrieve(ByteArray id);
    public List<Snapshot> retrieve(ByteArray prefixOrRange);

    /**
     * Difference between two snapshots in the timelapse.
     * Returns the set of changed vertex/edge IDs.
     * Used by ICE for bootstrap and iteration (§4).
     */
    public Delta diff(Snapshot a, Snapshot b);

    /**
     * Given a list of candidate vertices, expand the computation scope
     * by marking their 1-hop neighbors.
     * Used for implementing incremental computations (§4).
     */
    public SubgraphView expand(Set<Long> candidates, Snapshot snapshot);

    /**
     * Create a new snapshot using the union of vertices and edges
     * of two snapshots. For common vertices, run func to compute their value.
     * Used for implementing incremental computations (§4).
     */
    public Snapshot merge(Snapshot a, Snapshot b, MergeFunction func);
}
```

#### 4.3.2 Snapshot

```java
public final class Snapshot {
    private final GraphView graphView;
    private final ByteArray versionId;
    private final Timelapse timelapse;

    public GraphView graph();
    public ByteArray id();

    // Snapshot-aware graph operators (§6.2):
    // "It extends all the operators to operate on user-specified snapshot(s)"
    public Iterator<VertexData> vertices();
    public Iterator<EdgeData> edges();
    public <R> Snapshot mapVertices(Function<VertexData, R> fn);
    public <R> Snapshot mapEdges(Function<EdgeData, R> fn);
}
```

#### 4.3.3 Delta (Diff Result)

```java
public record Delta(
    Set<Long> addedVertices,
    Set<Long> removedVertices,
    Set<Long> modifiedVertices,
    Set<EdgeKey> addedEdges,
    Set<EdgeKey> removedEdges,
    Set<EdgeKey> modifiedEdges
) {
    /**
     * Returns the set of all "affected" vertex IDs:
     * added, removed, modified vertices, plus source/destination
     * vertices of added, removed, modified edges.
     */
    public Set<Long> affectedVertices();
}
```

The diff operation walks both pART trees concurrently, comparing shared vs. divergent subtrees. Since persistent data structures use structural sharing, shared subtrees (same object reference) can be skipped entirely — only divergent subtrees need comparison.

```java
public final class PersistentDiff {
    /**
     * Efficient diff exploiting structural sharing.
     * If two subtree roots are the same object reference, skip.
     * Only recurse into subtrees that differ.
     * Returns the set of keys that differ.
     */
    public static <V> Set<byte[]> diff(ArtNode<V> rootA, ArtNode<V> rootB);
}
```

#### 4.3.4 SubgraphView (Expand Result)

```java
public final class SubgraphView {
    private final Set<Long> activeVertices;      // vertices that must recompute
    private final Set<Long> boundaryVertices;    // 1-hop neighbors (for gather)
    private final GraphView backingGraph;

    public boolean isActive(long vertexId);
    public boolean isBoundary(long vertexId);
    public Iterator<VertexData> activeVertices();
    public Iterator<VertexData> boundaryVertices();
    public Iterator<EdgeData> relevantEdges();
}
```

#### 4.3.5 MergeFunction

```java
@FunctionalInterface
public interface MergeFunction {
    /**
     * For a vertex present in both snapshots, compute the merged value.
     * Used by ICE to copy state from previous computation for
     * vertices that did not recompute.
     */
    VertexData merge(VertexData fromA, VertexData fromB);
}
```

#### 4.3.6 Timelapse Creation and Lineage

The paper states (§3): *"Timelapses are created in Tegra in two ways — by the system and by the users."*

- **System-created**: When a new graph is introduced, a timelapse is created with a single snapshot. As the graph evolves, snapshots are added.
- **User-created**: During analytics, operations on snapshots create new snapshots. These may be added to existing timelapses or create new ones.
- **Lineage tracking**: The system tracks parent-child relationships between snapshots across timelapses.

```java
public final class TimelapseManager {
    private final PartitionStore store;
    private final Map<String, Timelapse> timelapses;

    public Timelapse create(String graphId);
    public Timelapse get(String graphId);
    public Timelapse branch(ByteArray snapshotId, String branchName);
    public LineageGraph lineage();
}
```

#### 4.3.7 Snapshot-Aware Graph Operators

**Paper reference**: §6.2

The paper states: *"It extends all the operators to operate on user-specified snapshot(s) (e.g., Graph.vertices(id) retrieves vertices at a given snapshot id, and Graph.mapV([ids]) can apply a map function on vertices of the graph on a set of snapshots)."*

```java
public final class TegraGraph {
    private final TimelapseManager manager;

    // Single-snapshot operators
    public Iterator<VertexData> vertices(ByteArray snapshotId);
    public Iterator<EdgeData> edges(ByteArray snapshotId);

    // Multi-snapshot operators (§3.1)
    // Enables temporal queries across multiple snapshots
    public <R> Map<ByteArray, R> mapVertices(
        List<ByteArray> snapshotIds, Function<VertexData, R> fn);

    // Graph-parallel computation on single snapshot
    public <M> Snapshot aggregateMessages(
        ByteArray snapshotId,
        SendMessageFunction<M> sendMsg,
        MergeMessageFunction<M> mergeMsg);

    // Graph-parallel computation across snapshots (§3.1)
    // "each processing phase is able to see the history of the node's property changes"
    public <M> Map<ByteArray, Snapshot> aggregateMessages(
        List<ByteArray> snapshotIds,
        SendMessageFunction<M> sendMsg,
        MergeMessageFunction<M> mergeMsg);
}
```

---

### 4.4 tegra-compute: ICE Computation Model & GAS Engine

**Paper reference**: §4, §6.1, Listing 1

#### 4.4.1 GAS (Gather-Apply-Scatter) Engine

**Paper reference**: §2.1, §6.1

The GAS model (PowerGraph [27]) decomposes vertex programs into three phases:

```java
public interface VertexProgram<V, E, M> {

    /**
     * Gather: collect information about adjacent vertices and edges
     * and apply a function on them.
     * @param context the edge triplet (src vertex, edge, dst vertex)
     * @return a message, or null if no message to send
     */
    M gather(EdgeTriplet<V, E> context);

    /**
     * Sum: combine messages from gather phase.
     * Associative and commutative.
     */
    M sum(M a, M b);

    /**
     * Apply: use the gathered/summed output to update the vertex value.
     */
    V apply(long vertexId, V currentValue, M gathered);

    /**
     * Scatter: given the new vertex value, determine which neighbors
     * should be activated in the next iteration.
     * @return set of neighbor IDs to activate, or empty to deactivate
     */
    Set<Long> scatter(EdgeTriplet<V, E> context, V newValue);

    /**
     * Specifies which neighbors to gather from (IN, OUT, BOTH).
     * Used by ICE diff() to determine affected vertices (§6.1).
     */
    EdgeDirection gatherNeighbors();

    /**
     * Specifies which neighbors are activated by scatter (IN, OUT, BOTH).
     * Used by ICE diff() to mark candidates for computation (§6.1).
     */
    EdgeDirection scatterNeighbors();
}

public enum EdgeDirection { IN, OUT, BOTH }

public record EdgeTriplet<V, E>(
    long srcId, V srcValue,
    long dstId, V dstValue,
    E edgeValue
) {}
```

#### 4.4.2 GAS Execution Engine

The engine iteratively applies GAS phases on a graph snapshot until convergence:

```java
public final class GasEngine {

    /**
     * Execute a vertex program on a single snapshot until convergence.
     * Stores intermediate iteration state in the timelapse (§4.2).
     *
     * @param snapshot the graph snapshot to compute on
     * @param program the vertex program
     * @param initialValues initial vertex values (or null for default)
     * @param maxIterations maximum iterations before forced termination
     * @param timelapse timelapse to store iteration state
     * @param algorithmId algorithm identifier for version ID generation
     * @return result snapshot with computed vertex values
     */
    public <V, E, M> Snapshot execute(
        Snapshot snapshot,
        VertexProgram<V, E, M> program,
        Map<Long, V> initialValues,
        int maxIterations,
        Timelapse timelapse,
        String algorithmId);
}
```

Execution loop:
1. Initialize all vertices with initial values or defaults.
2. Mark all vertices as active.
3. Repeat until no active vertices or maxIterations reached:
   a. **Gather**: For each active vertex, gather from `gatherNeighbors()`.
   b. **Sum**: Combine gathered messages per vertex.
   c. **Apply**: Update vertex values.
   d. **Scatter**: Determine next active set via `scatterNeighbors()`.
   e. **Save**: Store iteration state as snapshot in timelapse: `{snapshotId}_{algoId}_{iteration}`.
4. Return final result snapshot.

#### 4.4.3 ICE: Incremental Computation by Entity Expansion

**Paper reference**: §4.2

ICE operates in four phases:

**Phase 1 — Initial Execution**:
When an algorithm runs for the first time, ICE uses the standard GAS engine. It stores the state of vertices (and edges if needed) as properties in the graph, and at the end of every iteration, saves a snapshot to the timelapse. The ID is generated as `{graphId}_{timestamp}_{algoId}_{iteration}`.

**Phase 2 — Bootstrap**:
When the computation runs on a new snapshot S_{n+1}, ICE bootstraps from S_n's stored results:
1. Compute `delta = timelapse.diff(snapshot_n, snapshot_n_plus_1)`.
2. Identify affected vertices: changed vertices + source/destination of changed edges.
3. Mark `scatter_nbrs` of affected vertices as candidates (§6.1).
4. Expand candidates to include `gather_nbrs` (via `timelapse.expand()`).
5. This yields the subgraph on which GAS runs.

**Phase 3 — Iterations**:
At each iteration i:
1. Run GAS on the subgraph.
2. For vertices that did not recompute: copy state from `timelapse.retrieve({algoId}_{i})` (the stored iteration i from previous execution) via `timelapse.merge()`.
3. Compare the subgraph result with the stored iteration i to find the new subgraph for iteration i+1.
4. Expand the new affected set.

**Phase 4 — Termination**:
ICE terminates when BOTH conditions are met:
1. The subgraph has converged (no vertex changed state).
2. No entity needs its state copied from the stored timelapse snapshot (i.e., all stored iterations have been processed, or the remaining stored iterations match the current state).

If the new snapshot requires MORE iterations than the initial execution, ICE switches to full (non-incremental) GAS from that point.
If the new snapshot requires FEWER iterations, ICE still checks remaining stored iterations to copy state from.

```java
public final class IceEngine {

    private final GasEngine gasEngine;
    private final Timelapse timelapse;

    /**
     * Incremental Pregel implementation (Listing 1 from paper).
     *
     * @param graph the new graph snapshot to compute on
     * @param prevResult the timelapse containing previous execution results
     * @param program the vertex program
     * @param maxIterations maximum iterations
     * @return result snapshot
     */
    public <V, E, M> Snapshot incPregel(
        Snapshot graph,
        Timelapse prevResult,
        VertexProgram<V, E, M> program,
        int maxIterations);
}
```

The core loop, faithfully translated from Listing 1 in the paper:

```
function IncPregel(g, prevResult, vprog, sendMsg, gather):
    iter = 0
    while not converged:
        // Restrict to vertices that should recompute
        msgs = g.expand(g.diff(prevResult.retrieve(iter)))
                .aggregateMessages(sendMsg, gather)
        iter += 1
        // Receive messages and copy previous results
        g = g.leftJoinV(msgs).mapV(vprog)
             .merge(prevResult.retrieve(iter)).save(iter)
    return g
```

#### 4.4.4 ICE on GAS Decomposition

**Paper reference**: §6.1

The diff() API in ICE uses `scatter_nbrs` to determine which vertices must recompute:

1. Start with initial candidates (at bootstrap: graph changes; at iteration: candidates from previous iteration).
2. For each candidate, if its state differs from the previous iteration or from the previous execution stored in the timelapse, mark all its `scatter_nbrs` for computation.
3. A vertex addition inspects all its neighbors (as defined by `scatter_nbrs`) and includes them.

The expand() API uses `gather_nbrs` to include necessary neighbors:

1. For each candidate marked for recomputation, also mark its `gather_nbrs`.
2. These neighbors provide correct input for the `gather()` phase but do not recompute their own state (they are "boundary" vertices in the SubgraphView).

After diff and expand, Tegra has the complete subgraph for GAS computation.

#### 4.4.5 Multi-Snapshot Parallel Computation

**Paper reference**: §3.1, Figure 10

The paper describes running the same algorithm on a sequence of snapshots in parallel:

```java
public final class ParallelSnapshotExecutor {

    /**
     * Execute algorithm on multiple snapshots in parallel.
     * Uses virtual threads for concurrency.
     * First snapshot: full execution.
     * Subsequent snapshots: ICE incremental from the nearest computed result.
     *
     * Since snapshots are immutable and stored in DGSI,
     * multiple ICE computations can run concurrently on different snapshots.
     */
    public <V, E, M> Map<ByteArray, Snapshot> executeParallel(
        List<Snapshot> snapshots,
        VertexProgram<V, E, M> program,
        int maxIterations);
}
```

The paper (§3.1) explains that each processing phase operates on the evolution of an entity: *"the user-defined vertex program is provided with state in all the snapshots. Thus, we are able to eliminate redundant messages and computation."*

#### 4.4.6 Sharing State Across Queries

**Paper reference**: §4.3

The paper states: *"variants of connected components and pagerank algorithms both require the computation of vertex degree as one of the steps. Since ICE decouples state, such common computations can be stored as separate state that is shared across different queries."*

ICE enables modular state composition. Common sub-computations (e.g., vertex degrees) are stored as separate timelapse entries and referenced by multiple algorithms:

```java
public final class SharedStateRegistry {
    /**
     * Register a computation result that can be shared across queries.
     * Example: vertex degrees computed once, reused by PR and CC.
     */
    public void register(String computationId, ByteArray snapshotId);
    public Snapshot retrieve(String computationId, ByteArray snapshotId);
    public boolean exists(String computationId, ByteArray snapshotId);
}
```

#### 4.4.7 Learning-Based Switching

**Paper reference**: §4.3

The paper describes a random forest classifier that decides, at iteration boundaries, whether to switch from incremental to full re-execution:

> "We train a simple random forest classifier to predict, at the beginning of an iteration, if switching to full re-execution from that point would be faster compared to continuing with incremental execution."

**Training features** (recorded per iteration during offline training):
1. Number of vertices participating in computation
2. Average degree of active vertices
3. Number of partitions active
4. Number of messages generated per vertex
5. Number of messages received per vertex
6. Amount of data transferred over the network
7. Time taken for the iteration to complete

**Graph-specific features**:
8. Average degree of vertices
9. Average diameter
10. Clustering coefficient

**Label**: Whether switching to full recomputation in the next iteration resulted in faster execution.

```java
public final class SwitchingClassifier {
    private final RandomForestModel model;

    /**
     * Train the classifier offline using historical execution data.
     */
    public static SwitchingClassifier train(List<TrainingRecord> data);

    /**
     * Predict whether to switch to full re-execution at this iteration.
     */
    public boolean shouldSwitch(IterationMetrics metrics, GraphCharacteristics graphChars);

    /**
     * Load a pre-trained model from disk.
     */
    public static SwitchingClassifier load(Path modelPath);
    public void save(Path modelPath);
}

public record IterationMetrics(
    long activeVertexCount,
    double avgDegreeOfActiveVertices,
    int activePartitionCount,
    double msgsGeneratedPerVertex,
    double msgsReceivedPerVertex,
    long networkBytesTransferred,
    long iterationTimeMs
) {}

public record GraphCharacteristics(
    double avgDegree,
    double avgDiameter,
    double clusteringCoefficient
) {}

public record TrainingRecord(
    IterationMetrics metrics,
    GraphCharacteristics graphChars,
    boolean shouldSwitch  // label
) {}
```

The random forest implementation can use a lightweight Java ML library or a self-contained implementation (no heavy dependencies for an OSS core module).

#### 4.4.8 ICE Versatility: Monotonic Updates

**Paper reference**: §7.3, Figure 12

The paper notes: *"if updates are monotonic (only additions), then ICE can simply restart from the last answer instead of using full incremental computations."*

```java
public enum UpdateMonotonicity {
    ADDITIONS_ONLY,   // can restart from last answer
    DELETIONS_ONLY,
    MIXED             // requires full ICE
}
```

The ICE engine checks `UpdateMonotonicity` and uses the shortcut path for monotonic additions.

---

### 4.5 tegra-cluster: Distribution Layer

**Paper reference**: §5.2, §6

#### 4.5.1 Graph Partitioning

**Paper reference**: §5.2

The paper states: *"Tegra supports several graph partitioning schemes, similar to GraphX, to balance load and reduce communication. To distribute the graph across machines in the cluster, vertices are hash partitioned and edges are partitioned using one of many schemes."*

```java
public interface PartitionStrategy {
    int partitionForVertex(long vertexId, int numPartitions);
    int partitionForEdge(long srcId, long dstId, int numPartitions);
}

// Implementations matching GraphX partition strategies:
public final class HashPartitioning implements PartitionStrategy {
    // vertex and edge assigned by hash of vertex ID
}

public final class EdgePartition2D implements PartitionStrategy {
    // 2D partitioning: edges assigned by (srcId mod sqrt(P), dstId mod sqrt(P))
}

public final class RandomVertexCut implements PartitionStrategy {
    // edge assigned by hash of (srcId, dstId)
}

public final class CanonicalRandomVertexCut implements PartitionStrategy {
    // edge assigned by hash of (min(srcId,dstId), max(srcId,dstId))
}
```

The paper states: *"We do not partition the pART structures, instead Tegra partitions the graph and creates separate pART structures locally in each partition."*

Each cluster node runs its own `PartitionStore` instance with independent pART trees.

#### 4.5.2 Cluster Topology

```java
public final class ClusterManager {
    private final List<NodeDescriptor> nodes;
    private final PartitionStrategy strategy;
    private final int numPartitions;
    private final BarrierCoordinator barrierCoordinator;

    public void start();
    public void stop();
    public NodeDescriptor nodeForPartition(int partitionId);
    public int partitionForVertex(long vertexId);
}

public record NodeDescriptor(
    String host,
    int port,
    int nodeId,
    List<Integer> assignedPartitions
) {}
```

#### 4.5.3 Distributed Snapshot Protocol (Barrier)

**Paper reference**: §5.2

The paper states: *"all machines must commit snapshots at the same logical time. Tegra uses a lightweight barrier for this — after ingesting a batch of mutations, all partitions commit simultaneously to create a consistent distributed snapshot."*

```java
public final class BarrierCoordinator {
    /**
     * Coordinate a distributed snapshot commit across all partitions.
     * 1. Signal all partitions to prepare commit (flush pending mutations).
     * 2. Wait for all partitions to acknowledge readiness.
     * 3. Signal all partitions to commit with the same version ID.
     * 4. Wait for all commits to complete.
     */
    public CompletableFuture<ByteArray> coordinateCommit(ByteArray versionId);
}

public interface PartitionNode {
    /**
     * Called by the coordinator to trigger local commit preparation.
     */
    void prepareCommit(ByteArray versionId);

    /**
     * Called by the coordinator to finalize the commit.
     */
    void commit(ByteArray versionId);
}
```

#### 4.5.4 Distributed GAS Execution

**Paper reference**: §6

The paper states: *"We utilize the barrier execution mode to implement direct communication between tasks to avoid most Spark overheads."*

In distributed GAS:
- **Gather phase**: Local vertices gather from local neighbors directly. For remote neighbors (cross-partition edges), the vertex sends a request to the remote partition and receives the value.
- **Apply phase**: Purely local.
- **Scatter phase**: Sends updated values to remote partitions that hold neighbors needing activation.

```java
public final class DistributedGasEngine {
    private final ClusterManager cluster;
    private final MessageRouter messageRouter;

    /**
     * Execute a vertex program across all partitions.
     * Synchronizes iterations using barriers.
     */
    public <V, E, M> Map<Integer, Snapshot> execute(
        ByteArray snapshotId,
        VertexProgram<V, E, M> program,
        int maxIterations);
}

public final class MessageRouter {
    /**
     * Route GAS messages between partitions.
     * Uses direct task-to-task communication (not shuffle).
     */
    public <M> void sendMessage(int targetPartition, long vertexId, M message);
    public <M> Map<Long, M> receiveMessages(int partitionId);
    public void barrier();  // synchronize all partitions at iteration boundary
}
```

#### 4.5.5 Distributed ICE

**Paper reference**: §4

In distributed ICE:
1. Each partition computes its local diff.
2. Neighborhood expansion may cross partition boundaries. This requires one round of message passing to identify the full affected subgraph. A partition sends "expansion requests" for vertices that are remote, and receives back the neighbor lists.
3. Each partition then runs ICE locally on its affected vertices, with cross-partition gather/scatter handled by the `MessageRouter`.

```java
public final class DistributedIceEngine {
    private final DistributedGasEngine gasEngine;
    private final ClusterManager cluster;
    private final MessageRouter messageRouter;

    public <V, E, M> Map<Integer, Snapshot> incPregel(
        ByteArray snapshotId,
        ByteArray prevResultId,
        VertexProgram<V, E, M> program,
        int maxIterations);
}
```

#### 4.5.6 Update Ingestion and Routing

The paper states (§5.2): *"To consume updates, Tegra needs to send the updates to the right partition. Here, we impose the same partitioning as the original graph on the vertices/edges in the update."*

```java
public final class UpdateIngestionRouter {
    private final ClusterManager cluster;

    /**
     * Route a batch of mutations to their target partitions.
     * Each partition applies mutations to its working version.
     */
    public void ingest(List<GraphMutation> mutations);

    /**
     * After ingestion, trigger coordinated commit across all partitions.
     */
    public ByteArray commitAll(ByteArray versionId);
}
```

#### 4.5.7 Network Transport

For inter-partition communication, we use a simple RPC framework based on Java NIO with virtual threads:

```java
public interface TransportServer {
    void start(int port);
    void stop();
    void registerHandler(String method, RequestHandler handler);
}

public interface TransportClient {
    CompletableFuture<byte[]> send(NodeDescriptor target, String method, byte[] payload);
}
```

Messages are serialized using the `tegra-serde` module (§4.6).

---

### 4.6 tegra-serde: Serialization

#### 4.6.1 Purpose

Serialization is needed for two purposes in Tegra:
1. **Disk eviction**: Writing pART subtrees to disk and reading them back (§5.4).
2. **Network communication**: Serializing GAS messages and graph data for inter-partition transport.

The paper warns against JSON. We use a compact binary format.

#### 4.6.2 Binary Format

```java
public interface TegraSerializer<T> {
    void serialize(T value, DataOutput out) throws IOException;
    T deserialize(DataInput in) throws IOException;
    int estimateSize(T value);
}

// Built-in serializers
public final class ArtNodeSerializer<V> implements TegraSerializer<ArtNode<V>> { ... }
public final class VertexDataSerializer implements TegraSerializer<VertexData> { ... }
public final class EdgeDataSerializer implements TegraSerializer<EdgeData> { ... }
public final class PropertyMapSerializer implements TegraSerializer<PropertyMap> { ... }
public final class GasMessageSerializer<M> implements TegraSerializer<M> { ... }
```

#### 4.6.3 Subtree Serialization for Disk Eviction

The paper specifies (§5.4) that subtrees are written to separate files, and versions sharing subtrees share files on disk.

Each file contains a serialized subtree identified by its root node's identity hash. When writing a subtree:
1. Compute a content hash for the subtree root.
2. If a file with this hash already exists (shared by another version), skip writing.
3. Otherwise, serialize the subtree depth-first to a binary file.
4. Replace the in-memory subtree root with a `DiskNode` pointing to the file.

Deserialization is lazy: `DiskNode.lookup()` triggers materialization of the subtree from disk on first access.

#### 4.6.4 Adaptive Leaf Sizes

The paper mentions (Figure 6 caption): *"Data structure uses adaptive leaf sizes for efficiency."*

Leaves that store small property maps are serialized inline with their parent nodes. Leaves with large property maps are stored in separate files. The threshold is configurable.

---

### 4.7 tegra-algorithms: Graph Algorithms

**Paper reference**: §7, Table 5

The paper evaluates Tegra with the following algorithms, all implemented on the GAS model. Each algorithm is a `VertexProgram` implementation:

#### 4.7.1 Connected Components (CC) — Label Propagation

```java
public final class ConnectedComponents implements VertexProgram<Long, Object, Long> {
    // gather: min of neighbor labels
    // apply: update label to min(current, gathered)
    // scatter: activate neighbors if label changed
    // Converges when no labels change
}
```

The paper explicitly uses label propagation for CC (not union-find): *"DD uses a much superior union-find approach to CC while Tegra and GraphBolt use an iterative approach."*

#### 4.7.2 PageRank (PR)

```java
public final class PageRank implements VertexProgram<Double, Object, Double> {
    private final double dampingFactor;   // typically 0.85
    private final double tolerance;       // convergence threshold
    private final int maxIterations;      // paper: 20

    // gather: sum of (neighbor_rank / neighbor_out_degree)
    // apply: (1 - d) + d * gathered
    // scatter: activate neighbors if rank changed > tolerance
    // Paper: "run PR until specific convergence or 20 iterations, whichever is lower"
}
```

#### 4.7.3 Belief Propagation (BP)

```java
public final class BeliefPropagation implements VertexProgram<double[], Object, double[]> {
    // Generalized belief propagation (Yedidia et al., reference [74])
    // gather: collect belief messages from neighbors
    // apply: update beliefs using collected messages
    // scatter: send updated beliefs to neighbors
}
```

#### 4.7.4 Label Propagation (LP)

```java
public final class LabelPropagation implements VertexProgram<Long, Object, Map<Long, Long>> {
    // gather: collect label frequencies from neighbors
    // apply: adopt most frequent label
    // scatter: activate neighbors if label changed
}
```

#### 4.7.5 Collaborative Filtering (CF)

```java
public final class CollaborativeFiltering implements VertexProgram<double[], Double, double[]> {
    private final int numFactors;
    // ALS-style matrix factorization on bipartite graph
    // gather: collect factor vectors from neighbors
    // apply: solve least squares for this vertex's factors
    // scatter: activate neighbors if factors changed significantly
}
```

#### 4.7.6 Triangle Count (TC)

```java
public final class TriangleCount implements VertexProgram<Long, Object, Set<Long>> {
    // gather: collect neighbor ID sets
    // apply: count intersections with own neighbors
    // scatter: activated by edge changes only (no iteration cascade)
    // Paper notes: "incremental computations are simple... involves just updating a count"
}
```

#### 4.7.7 Co-Training Expectation Maximization (CoEM)

```java
public final class CoTrainingEM implements VertexProgram<double[], Double, double[]> {
    // Paper uses "the Latent Dirichlet Allocation (LDA) implementation
    // in GraphX which uses EM"
    // Implements EM update rules on factor graph
}
```

#### 4.7.8 Breadth-First Search (BFS)

```java
public final class BreadthFirstSearch implements VertexProgram<Integer, Object, Integer> {
    private final long sourceVertex;
    // gather: min(neighbor_distance + 1)
    // apply: update distance
    // scatter: activate unvisited neighbors
    // Paper notes: "light weight... only a very small part of the graph to be active"
}
```

#### 4.7.9 k-Hop

```java
public final class KHop implements VertexProgram<Set<Long>, Object, Set<Long>> {
    private final long sourceVertex;
    private final int k;
    // Compute all vertices within k hops of source
    // gather: collect hop sets from neighbors
    // apply: union with own set
    // scatter: propagate if set changed and within k hops
    // Paper evaluation uses k=4
}
```

---

### 4.8 tegra-benchmark: Evaluation Harness

**Paper reference**: §7

The benchmark module reproduces the evaluation from the paper.

#### 4.8.1 Datasets

The paper uses (Table 2):
- **Twitter**: 41.6M vertices / 1.47B edges
- **UK-2007**: 105.9M vertices / 3.74B edges
- **Synthetic (Facebook)**: Varies / 5B, 10B, 50B edges

For testing: SNAP datasets (smaller Twitter/LiveJournal subsets) and synthetic R-MAT power-law graphs.

```java
public interface DatasetLoader {
    GraphSource load(String datasetName, Path dataPath);
}

public final class SnapDatasetLoader implements DatasetLoader { ... }
public final class RmatGraphGenerator implements DatasetLoader {
    // R-MAT power-law graph generator
    // Parameters: scale, edgeFactor, a=0.57, b=0.19, c=0.19, d=0.05
}
```

#### 4.8.2 Workload Generation

The paper simulates temporal evolution: *"start with 80% of edges and add 1% per snapshot"* (§7.3), and for ad-hoc analysis: *"each update modifies 0.1% of the edges (adds and removes equal number)"* (§7.2).

```java
public final class WorkloadGenerator {
    /**
     * Generate a sequence of graph mutations simulating temporal evolution.
     */
    public List<List<GraphMutation>> generateEvolution(
        GraphView baseGraph,
        double mutationRate,    // e.g., 0.01 for 1%
        int numSnapshots,
        MutationType type       // ADDITIONS_ONLY, EQUAL_ADD_REMOVE, etc.
    );
}
```

#### 4.8.3 Experiments

Reproducing the paper's experiments:

| Experiment | Paper Section | Measures |
|-----------|--------------|----------|
| Snapshot retrieval latency | §7.1, Table 3 | Avg latency for 10 random retrievals with 200-1000 snapshots |
| Computation state storage | §7.1, Figure 7 | Memory usage after 200-1000 incremental computations |
| Ad-hoc single snapshot | §7.2, Figure 8 | Avg query time on 100 random single-snapshot windows |
| Ad-hoc window (size 10) | §7.2, Figure 9 | Avg query time on 100 random windows of 10 snapshots |
| Large graphs | §7.2, Table 4 | PR/CC/BP on 5B, 10B, 50B edge graphs |
| Batch size effect | §7.2, Table 5 | Performance with 1K, 10K, 100K batch sizes |
| Parallel snapshots | §7.3, Figure 10 | Temporal query on 2-20 snapshots, CC algorithm |
| Switching capability | §7.3, Figure 11 | ICE with/without switching after targeted deletions |
| Monotonic updates | §7.3, Figure 12 | ICE with additions-only vs full incremental |
| State sharing | §7.3, Figure 13 | Memory/runtime with/without state sharing between CC and PR |
| Streaming analysis | §7.4, Figure 14 | Online CC with continuous small updates |
| Temporal analysis | §7.4, Figure 15 | Purely temporal window-10 query vs Chlonos-style |

```java
public final class BenchmarkRunner {
    public void runAll(BenchmarkConfig config);
    public BenchmarkResult runExperiment(String experimentName, BenchmarkConfig config);
    public void exportResults(Path outputDir);  // CSV + summary
}
```

#### 4.8.4 Metrics Collection

```java
public final class MetricsCollector {
    public void recordLatency(String operation, long nanos);
    public void recordMemory(String label, long bytes);
    public void recordThroughput(String operation, long count, long nanos);
    public MetricsSnapshot snapshot();
}
```

---

## 5. Java 21 Platform Mapping

### 5.1 Virtual Threads (Project Loom)

Tegra naturally benefits from virtual threads in several places:

| Use Case | Paper Basis | Java 21 Mechanism |
|----------|-------------|-------------------|
| Parallel snapshot queries | §3.1, Figure 10 | `Executors.newVirtualThreadPerTaskExecutor()` |
| LRU eviction background thread | §5.4 | `Thread.ofVirtual().start()` |
| Orphan cleanup | §5.4 | `Thread.ofVirtual().start()` |
| Distributed GAS message handling | §6 | Virtual thread per incoming RPC |
| Barrier coordination | §5.2 | Virtual thread per partition coordinator |

Virtual threads are ideal because Tegra's workloads are I/O-mixed (disk reads for evicted snapshots, network for distributed GAS). Virtual threads avoid the overhead of platform thread pools while enabling simple blocking I/O code.

### 5.2 Records and Sealed Interfaces

Java 21 records provide immutable, value-based semantics perfect for Tegra's data model:

- `VertexData`, `EdgeData`, `EdgeKey`, `EdgeTriplet` — immutable graph entities
- `ByteArray`, `DiskPointer`, `NodeDescriptor` — system identifiers
- `Delta`, `IterationMetrics`, `GraphCharacteristics`, `TrainingRecord` — computation data
- `PropertyValue` subtypes — tagged union for property values

Sealed interfaces provide exhaustive pattern matching for node types:

```java
public sealed interface ArtNode<V>
    permits Node4, Node16, Node48, Node256, Leaf, DiskNode {}

// Pattern matching in traversal:
switch (node) {
    case Node4<V> n4 -> handleNode4(n4);
    case Node16<V> n16 -> handleNode16(n16);
    case Node48<V> n48 -> handleNode48(n48);
    case Node256<V> n256 -> handleNode256(n256);
    case Leaf<V> leaf -> handleLeaf(leaf);
    case DiskNode<V> disk -> handleDisk(disk);
}
```

### 5.3 Foreign Memory API (Panama)

The Foreign Function & Memory API (`java.lang.foreign`) in Java 21 enables off-heap memory management, critical for Tegra's tree nodes to avoid GC pressure.

**Use cases**:

1. **ART node storage**: Allocate Node4/16/48/256 structures off-heap using `Arena` and `MemorySegment`. This avoids GC scanning of the large persistent tree structures.

2. **Bulk allocation during path-copying**: Use `Arena.ofConfined()` for transient nodes during branch-commit windows. All transient allocations are freed together when the arena is closed.

3. **Memory-mapped disk access**: Use `MemorySegment.mapFile()` for zero-copy access to evicted subtrees.

```java
// Example: Off-heap Node256 layout
public final class OffHeapNode256<V> implements ArtNode<V> {
    private static final MemoryLayout LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("refCount"),
        ValueLayout.JAVA_INT.withName("prefixLen"),
        MemoryLayout.sequenceLayout(8, ValueLayout.JAVA_BYTE).withName("prefix"),
        ValueLayout.JAVA_SHORT.withName("numChildren"),
        MemoryLayout.sequenceLayout(256, ValueLayout.ADDRESS).withName("children")
    );

    private final MemorySegment segment;
    // ...
}
```

Note: Off-heap node storage is an optimization path. The initial implementation should use standard Java objects, with off-heap as a configurable option for production workloads requiring predictable GC behavior.

### 5.4 Structured Concurrency

Java 21's structured concurrency (preview) is a natural fit for parallel snapshot computation:

```java
// Parallel computation across snapshots (§3.1)
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
    List<Subtask<Snapshot>> tasks = snapshots.stream()
        .map(s -> scope.fork(() -> iceEngine.compute(s, program)))
        .toList();
    scope.join().throwIfFailed();
    return tasks.stream().collect(toMap(
        t -> t.get().id(),
        Subtask::get
    ));
}
```

### 5.5 Concurrency Utilities

- `StampedLock` for version map access (optimistic reads for retrieval, write lock for commit).
- `VarHandle` for atomic refCount operations on ART nodes.
- `ConcurrentHashMap` for version map and shared state registry.
- `ReentrantReadWriteLock` for coordinated barrier operations.

---

## 6. Build System & Project Layout

### 6.1 Gradle Configuration

Root `settings.gradle.kts`:

```kotlin
rootProject.name = "tegra-j"

include(
    "tegra-pds",
    "tegra-serde",
    "tegra-store",
    "tegra-api",
    "tegra-compute",
    "tegra-cluster",
    "tegra-algorithms",
    "tegra-benchmark",
    "tegra-examples"
)
```

Root `build.gradle.kts`:

```kotlin
plugins {
    java
    `java-library`
    `maven-publish`
    id("com.diffplug.spotless") version "6.25.0"
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
        modularity.inferModulePath.set(true)
    }

    tasks.withType<JavaCompile> {
        options.compilerArgs.addAll(listOf(
            "--enable-preview",
            "-Xlint:all"
        ))
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        jvmArgs("--enable-preview")
    }

    group = "org.tegra"
    version = "0.1.0-SNAPSHOT"
}
```

### 6.2 External Dependencies (Minimal)

To keep the core lightweight and suitable for OSS:

| Dependency | Module | Purpose |
|-----------|--------|---------|
| JUnit 5 | all (test) | Testing |
| JMH | tegra-benchmark | Microbenchmarks |
| SLF4J + Logback | all | Logging |
| JCTools | tegra-pds, tegra-store | Lock-free concurrent data structures |
| (none for ML) | tegra-compute | Random forest: self-contained implementation |

No framework dependencies (no Spring, no Netty for core). Network transport uses Java NIO directly with virtual threads.

### 6.3 Directory Structure per Module

```
tegra-{module}/
├── build.gradle.kts
└── src/
    ├── main/
    │   ├── java/
    │   │   └── org/tegra/{module}/
    │   │       └── *.java
    │   └── resources/
    │       └── module-info.java
    └── test/
        └── java/
            └── org/tegra/{module}/
                └── *Test.java
```

---

## 7. Testing Strategy

### 7.1 Unit Tests

Every module has comprehensive unit tests. Key areas:

**tegra-pds**:
- ART insert/lookup/delete correctness for all node types.
- Path-copying: verify old root unchanged after insert.
- Structural sharing: verify shared nodes have same identity.
- Node transitions: Node4→16→48→256 and back.
- Prefix iteration correctness.
- Range iteration correctness.
- Reference counting accuracy.
- Bulk builder correctness.
- Memory: create 1000 versions with small mutations, verify memory is sublinear (paper §7.1 style).

**tegra-store**:
- Branch/commit lifecycle.
- Version map operations (put, get, prefix match, range match).
- LRU eviction: access patterns, eviction ordering.
- Disk serialization roundtrip.
- Orphan cleanup timing.
- Concurrent read access to shared snapshots.

**tegra-api**:
- Timelapse save/retrieve roundtrip.
- Diff: known graph changes produce expected delta.
- Diff efficiency: shared subtrees skipped.
- Expand: 1-hop neighbors correctly identified.
- Merge: correct vertex resolution.

**tegra-compute**:
- GAS engine convergence on small graphs.
- ICE correctness: incremental result matches full recomputation.
- ICE termination conditions.
- Switching classifier: correct predictions on known data.
- Parallel snapshot execution: results match serial execution.

**tegra-algorithms**:
- Each algorithm against known results on small graphs.
- Incremental results match full recomputation after mutations.

### 7.2 Integration Tests

- End-to-end: load graph → create snapshots → run algorithms → verify results.
- Distributed: multi-partition setup (in-process) with cross-partition edges.
- Eviction: create many snapshots, trigger eviction, verify retrieval from disk.
- Concurrent access: multiple virtual threads querying/modifying simultaneously.

### 7.3 Property-Based Tests

- For any sequence of insert/delete operations, the pART always returns correct lookup results.
- For any two snapshots, diff followed by applying the delta to snapshot A produces snapshot B.
- ICE on any graph mutation sequence produces the same result as full recomputation.

### 7.4 Performance Tests (JMH)

- pART insert/lookup throughput at various tree sizes.
- Path-copying overhead vs. full-copy baseline.
- Structural sharing memory savings.
- GAS iteration throughput.
- ICE speedup vs. full recomputation at various mutation rates.

---

## 8. Fault Tolerance

**Paper reference**: §6

The paper states: *"Spark provides fault tolerance by checkpointing inputs and operations for reconstructing the state. Tegra provides coarse-grained fault tolerance by leveraging Spark's rdd.checkpoint semantics. Users can explicitly run checkpoint operation, upon which Tegra flushes the contents in DGSI to persistent storage."*

And: *"We currently do not support fine-grained lineage-based fault tolerance provided by Spark."*

Our implementation provides the same coarse-grained checkpoint mechanism:

```java
public final class CheckpointManager {
    private final DiskStore diskStore;

    /**
     * Flush all DGSI contents to persistent storage.
     * User-triggered operation.
     */
    public void checkpoint(PartitionStore store);

    /**
     * Restore DGSI state from a checkpoint.
     */
    public PartitionStore restore(Path checkpointDir);
}
```

Fine-grained lineage-based fault tolerance is explicitly out of scope per the paper.

---

## 9. OSS Release Plan

### 9.1 License

Apache License 2.0 — standard for Apache Foundation projects, permissive, patent-grant included.

### 9.2 Project Metadata

- **Group ID**: `org.tegra`
- **Artifact prefix**: `tegra-`
- **Minimum Java**: 21
- **SCM**: GitHub
- **CI**: GitHub Actions (build + test on JDK 21, 22)
- **Code style**: Google Java Style (enforced via Spotless)
- **Static analysis**: Error Prone, SpotBugs

### 9.3 API Stability

- Public APIs in `tegra-api` and `tegra-algorithms` are the user-facing contracts.
- Internal modules (`tegra-pds`, `tegra-store`, `tegra-serde`) are exported only to other Tegra modules via JPMS `exports ... to` directives.
- SPI interfaces (`GraphImporter`, `PartitionStrategy`, `VertexProgram`) are stable extension points.
- Versioning follows SemVer.

### 9.4 Documentation

- Javadoc on all public APIs.
- Architecture guide (this document, condensed).
- Getting started guide with examples.
- Benchmark reproduction guide.

### 9.5 Release Artifacts

Published to Maven Central:
- `tegra-api` — for algorithm developers
- `tegra-algorithms` — standard algorithms
- `tegra-cluster` — for distributed deployments
- `tegra-benchmark` — for reproducibility

---

## 10. Implementation Phases

### Phase 1: Core Storage (tegra-pds + tegra-serde + tegra-store)

**Goal**: Single-node persistent graph store with versioning.

1. Implement persistent ART with all four node types, path-copying, reference counting, and path compression.
2. Implement binary serialization for ART nodes and graph data.
3. Build the graph data model (vertex pART + edge pART) on top of pART.
4. Implement version management: branch, commit, retrieve, version ID matching.
5. Implement diff operation exploiting structural sharing.
6. Implement LRU eviction with disk serialization.
7. Implement mutation log.

**Validation**: Create 1000 snapshots with 1% edge mutations, verify sublinear memory. Verify diff correctness and structural sharing.

### Phase 2: Timelapse API (tegra-api)

**Goal**: User-facing abstraction layer.

1. Implement Timelapse: save, retrieve, diff, expand, merge.
2. Implement TimelapseManager for timelapse lifecycle.
3. Implement snapshot-aware graph operators.
4. Implement VersionIdGenerator for automatic ID schemes.

**Validation**: End-to-end test: create graph → save snapshots → retrieve → diff → expand → merge.

### Phase 3: Graph Computation (tegra-compute + tegra-algorithms)

**Goal**: GAS engine and ICE incremental computation.

1. Implement GAS engine with full execution on single snapshots.
2. Implement all 9 algorithms as VertexProgram implementations.
3. Verify correctness on small known graphs.
4. Implement ICE: bootstrap, iterations, termination.
5. Implement parallel snapshot execution.
6. Implement shared state registry.
7. Implement monotonic update shortcut.
8. Implement learning-based switching classifier.

**Validation**: For each algorithm, verify ICE result matches full recomputation on 100 random mutation sequences. Verify parallel execution matches serial.

### Phase 4: Distribution (tegra-cluster)

**Goal**: Multi-node distributed execution.

1. Implement partition strategies (hash, 2D, random vertex cut).
2. Implement network transport (Java NIO + virtual threads).
3. Implement distributed GAS with cross-partition messaging.
4. Implement barrier snapshot protocol.
5. Implement distributed ICE with cross-partition expansion.
6. Implement update ingestion routing.

**Validation**: Multi-partition in-process tests. Distributed integration tests with multiple JVM processes.

### Phase 5: Benchmarking (tegra-benchmark)

**Goal**: Reproduce paper evaluation results.

1. Implement dataset loaders (SNAP, R-MAT generator).
2. Implement workload generator (temporal evolution, ad-hoc queries).
3. Implement all experiments from §7.
4. Produce comparison metrics against baseline (full recomputation).

### Phase 6: Hardening & Release

1. Performance profiling and optimization.
2. Stress testing (large graphs, many snapshots, high concurrency).
3. Documentation.
4. CI/CD pipeline.
5. Maven Central publication.

---

## 11. Risk Analysis

### 11.1 GC Pressure from Path-Copying

**Risk**: High allocation rate during bulk mutations creates GC pauses.
**Mitigation**: In-place updates during branch-commit (paper §5.4). Object pooling. Optional off-heap storage via Foreign Memory API. Bulk builder for initial load.

### 11.2 Cross-Partition ICE Cascade

**Risk**: Neighborhood expansion in ICE crosses partition boundaries, causing cascading expansion.
**Mitigation**: The paper notes this is a known concern. Expansion is limited to 1-hop neighbors per iteration. Multi-hop propagation happens across iterations, naturally bounded by convergence.

### 11.3 Memory Fragmentation

**Risk**: Many small ART node allocations fragment the heap.
**Mitigation**: Arena allocators for transient nodes. Object pooling with size classes matching Node4/16/48/256. G1GC or ZGC for low-pause collection.

### 11.4 Snapshot Consistency in Distribution

**Risk**: Stragglers delay the barrier commit protocol.
**Mitigation**: Configurable timeout on barrier wait. The paper uses a "lightweight barrier" — we implement it with a simple two-phase protocol (prepare + commit) with timeout and rollback on failure.

### 11.5 Disk Eviction Latency

**Risk**: Accessing an evicted snapshot causes latency spike.
**Mitigation**: Lazy materialization (only load accessed subtrees). Memory-mapped files for fast access. Prefetching heuristics for likely-accessed snapshots.

### 11.6 Random Forest Model Portability

**Risk**: The switching classifier trained on one graph/algorithm may not generalize.
**Mitigation**: The paper includes graph-specific features (degree, diameter, clustering coefficient) in the model. Per-graph model training is supported. Fallback to full recomputation if model is unavailable.
