# Tegra: Implementation Guide for Claude Code

## Paper Reference

**TEGRA: Efficient Ad-Hoc Analytics on Evolving Graphs**
Anand Padmanabha Iyer, Qifan Pu, Kishan Patel, Joseph E. Gonzalez, Ion Stoica
NSDI 2021 (18th USENIX Symposium on Networked Systems Design and Implementation)
From UC Berkeley RISE Lab / Microsoft Research

PDF: https://www.usenix.org/conference/nsdi21/presentation/iyer https://www.usenix.org/system/files/nsdi21-iyer.pdf

---

## Why This Paper

Tegra solves a specific and compelling problem: how do you run ad-hoc graph analytics on *arbitrary time windows* of an evolving graph without recomputing everything from scratch? Most graph systems either process a single static snapshot or support only streaming on the latest version. Tegra lets you "time travel" — run PageRank on last Tuesday's graph, compare connected components between two snapshots a week apart, or analyze a sliding window over the past hour — all efficiently on a distributed cluster.

The key insight is combining **persistent data structures** (structural sharing across graph versions) with **incremental computation** (reusing intermediate results across snapshots). This combination is architecturally elegant and very implementable.

---

## System Architecture Overview

Tegra has three main components:

```
┌─────────────────────────────────────────────────┐
│                   Timelapse API                  │
│  (User-facing abstraction: sequence of snapshots)│
└─────────────┬───────────────────┬───────────────┘
              │                   │
              ▼                   ▼
┌─────────────────────┐ ┌────────────────────────┐
│        DGSI         │ │         ICE            │
│  Distributed Graph  │ │  Incremental Compute   │
│   Snapshot Index    │ │  by Entity Expansion   │
│  (versioned store)  │ │  (computation model)   │
└─────────────────────┘ └────────────────────────┘
```

---

## Component 1: Timelapse (The User Abstraction)

### What it is

Timelapse is the user-facing API. A Timelapse is a logical sequence of immutable graph snapshots, each representing the graph at a point in time. Users interact with Timelapses rather than raw graph mutations.

### Core API

```
timelapse = Timelapse(graph_id)

# Create a new snapshot from the current graph state
snapshot = timelapse.commit()

# Retrieve a snapshot by ID or timestamp
snapshot = timelapse.get("TWTR_1577869200")

# Retrieve a range of snapshots (prefix matching)
snapshots = timelapse.range("TWTR_157786", "TWTR_157800")

# Branch from an existing snapshot (for what-if analysis)
branch = timelapse.branch(snapshot_id)

# Run a graph computation on one snapshot
result = snapshot.run(PageRank, iterations=20)

# Run computation across a sequence of snapshots in parallel
results = timelapse.map(snapshots, ConnectedComponents)

# Sliding window query
results = timelapse.window(start, end, stride, PageRank)
```

### Key Design Decisions

- Snapshots are **immutable** — once committed, they never change
- Snapshot IDs use a naming scheme like `GRAPHID_UNIXTIMESTAMP` — prefix matching gives you all snapshots for a graph, range queries give you time windows
- Between snapshots, raw mutations are stored in a simple log (not versioned) — the log is only needed to reconstruct intermediate states if required
- Branching enables what-if analysis: fork a snapshot, apply hypothetical mutations, run analytics

### Implementation Priority: HIGH
This is the entry point for all user interaction. Implement it as a thin layer on top of DGSI.

---

## Component 2: DGSI (Distributed Graph Snapshot Index)

### What it is

DGSI is the core storage engine. It's a distributed, versioned graph store that uses **persistent data structures** to share structure between graph versions, making it memory-efficient to store many snapshots simultaneously.

### The Key Idea: Persistent Data Structures

A persistent data structure preserves its previous versions when modified. Instead of mutating in place, modifications create a new version that shares most of its structure with the old version.

Tegra uses a persistent variant of the **Adaptive Radix Tree (ART)** as its underlying data structure. When a graph mutation happens (e.g., changing a vertex property from "Foo" to "Bar"), only the affected tree path is copied — all other nodes are shared between the old and new versions.

```
Snapshot 1 (v1)         Snapshot 2 (v2)
     root1                   root2
    /    \                  /    \
   A      B               A'     B     ← B is shared
  / \                    / \
 C   D                  C   D'         ← C is shared, D' is new
```

In this example, modifying vertex D creates a new root and a new path to D', but nodes B and C are shared between v1 and v2. For a graph with millions of vertices where only a few change between snapshots, this saves enormous amounts of memory.

### Data Layout

Each partition in DGSI stores:
- A **version map**: snapshot_id → root of the ART for that version
- The **ART nodes** themselves, with reference counting for garbage collection
- **Vertex data**: properties, adjacency lists (stored as leaves of the ART)
- **Edge data**: stored alongside source vertex or in a separate edge partition

### Key Operations

**commit(graph_state) → snapshot_id**
1. Take the current mutable graph state
2. Create a new ART version by path-copying only the modified nodes
3. Register the new root in the version map
4. Return the snapshot ID

**get(snapshot_id) → graph_view**
1. Look up the root in the version map
2. Return a read-only view backed by that ART root
3. Traversal follows the tree structure — shared nodes are transparently accessed

**branch(snapshot_id) → mutable_copy**
1. Create a new entry in the version map pointing to the same root
2. Future modifications to the branch will path-copy as usual
3. The original snapshot remains unchanged

**evict(snapshot_id)**
1. Remove the root from the version map
2. Decrement reference counts on all nodes reachable only from this root
3. Nodes with zero references are freed
4. For LRU eviction: serialize subtrees to disk, replace in-memory pointers with file references

### Distribution Strategy

The graph is hash-partitioned across machines by vertex ID. Each machine runs its own DGSI instance managing its partition. Cross-partition edges are stored on both endpoints (or on the source, depending on the algorithm).

Important: all machines must commit snapshots at the same logical time. Tegra uses a lightweight barrier for this — after ingesting a batch of mutations, all partitions commit simultaneously to create a consistent distributed snapshot.

### Memory Management

- **LRU eviction**: Each snapshot access is timestamped. A background thread periodically evicts least-recently-used snapshots to disk.
- **Disk serialization**: Each version's unique subtrees are written to separate files. Shared nodes across versions share files on disk too.
- **Lazy materialization**: Only actively queried snapshots need to be fully in memory. Others can be partially on disk.

### Implementation Priority: HIGHEST
This is the most technically challenging and novel component. The persistent ART is the core innovation. A simpler starting point would be to use a persistent balanced tree (e.g., a persistent red-black tree or a persistent HAMTrie) instead of a full ART — the structural sharing idea is the same.

### Simplification for v1

Instead of implementing a full persistent ART, start with a **persistent Hash Array Mapped Trie (HAMT)** — the same data structure used by Clojure and Scala's immutable collections. HAMTs are simpler to implement and provide the same structural sharing properties. You can upgrade to ART later for better cache performance.

---

## Component 3: ICE (Incremental Computation by Entity Expansion)

### What it is

ICE is Tegra's computation model. It enables running graph algorithms incrementally across snapshots, avoiding full recomputation when the graph changes slightly.

### The Key Idea

When a graph changes between snapshot S_n and S_{n+1}, only a small subgraph is affected. ICE identifies this subgraph and runs the graph computation only on the affected neighborhood, bootstrapping from the previous snapshot's results.

### How ICE Works

1. **Identify changed entities**: Compare S_n and S_{n+1} to find added/removed vertices and edges
2. **Expand neighborhood**: For each changed entity, include its k-hop neighborhood in the "affected subgraph" (k depends on the algorithm — typically 1-hop for PageRank, more for algorithms with longer-range dependencies)
3. **Bootstrap from previous results**: Copy the computation state (vertex values, intermediate aggregations) from S_n for all vertices in the affected subgraph
4. **Recompute locally**: Run the graph algorithm on the affected subgraph, using bootstrapped values as initialization
5. **Merge results**: Combine the recomputed values for affected vertices with the unchanged values from S_n

```
Snapshot S_n          Change: add edge A→D and vertex D
  A → B                         
  B → C              Affected subgraph (1-hop expansion):
                       A, D, and A's neighbors (B)
                       
Snapshot S_{n+1}      ICE recomputes only {A, B, D}
  A → B              using S_n's values for B and C
  A → D              as initialization
  B → C
```

### ICE for Parallel Snapshot Computation

When you need to run an algorithm across many snapshots (e.g., "give me PageRank for every hour in the last day"), ICE can process them in parallel:

1. Compute the full result for the first snapshot
2. For each subsequent snapshot, run ICE incrementally from the previous result
3. Since snapshots are immutable and stored in DGSI, multiple ICE computations can run concurrently on different snapshots

### ICE also works for iteration sharing

Graph algorithms like PageRank are iterative — they run multiple rounds until convergence. ICE observes that iterations of a single computation are themselves a sequence of "snapshots" (each iteration modifies vertex values). So Tegra stores intermediate iteration states in DGSI too, enabling:
- Resuming a computation from a checkpoint
- Sharing intermediate states across queries that compute the same algorithm on similar snapshots

### Integration with GAS (Gather-Apply-Scatter)

Tegra focuses on the GAS programming model:
- **Gather**: Collect values from neighbors
- **Apply**: Update the vertex's value
- **Scatter**: Propagate updates to neighbors

ICE modifies this by restricting Gather/Apply/Scatter to only the affected subgraph, and initializing vertex values from the previous snapshot's results rather than from scratch.

### Implementation Priority: MEDIUM-HIGH
ICE is conceptually simpler than DGSI but requires careful integration with the storage layer. Start with a naive version (recompute fully per snapshot) and optimize to incremental later.

---

## Suggested Implementation Plan

### Phase 1: Core Storage (Days 1-2)

**Goal**: Implement a single-node persistent graph store with versioning.

1. Implement a **persistent HAMT** (Hash Array Mapped Trie) in your chosen language
   - Key operation: `insert(key, value) → new_root` (returns new root, old root unchanged)
   - Key property: structural sharing between versions
   - Test: create 1000 versions with small mutations, verify memory usage is sublinear

2. Build the **graph data model** on top of the HAMT
   - Vertex: `{id, properties: Map, neighbors: List<EdgeId>}`
   - Edge: `{src, dst, properties: Map}`
   - Store vertices in the HAMT keyed by vertex ID
   - Store edges in a separate HAMT keyed by edge ID (or inline with source vertex)

3. Implement **snapshot operations**
   - `commit()`: freeze current state, create snapshot ID, store root
   - `get(snapshot_id)`: return read-only graph view from stored root
   - `branch(snapshot_id)`: create writable copy from existing snapshot
   - `diff(snap_a, snap_b)`: return set of changed vertex/edge IDs

### Phase 2: Graph Computation (Days 2-3)

**Goal**: Implement basic graph algorithms on static snapshots, then add ICE.

1. Implement a **GAS engine** on a single snapshot
   - Start with PageRank and Connected Components
   - Run iteratively until convergence
   - Store results as vertex properties

2. Implement **Timelapse operations**
   - `timelapse.map(snapshots, algorithm)`: run algorithm on each snapshot
   - Start with naive full-recomputation per snapshot
   
3. Add **ICE incremental computation**
   - Implement `diff()` between consecutive snapshots
   - Implement k-hop neighborhood expansion from changed entities
   - Modify GAS engine to accept initial values from previous snapshot
   - Restrict computation to affected subgraph + expansion

### Phase 3: Distribution (Days 3-4)

**Goal**: Distribute the system across multiple nodes.

1. **Partition the graph** by vertex ID (hash partitioning)
   - Each node runs its own DGSI instance
   - Cross-partition edges require message passing

2. **Distributed snapshot protocol**
   - After ingesting a batch of mutations, coordinate a barrier across all partitions
   - All partitions commit simultaneously → consistent distributed snapshot
   - Use a simple coordinator node for the barrier

3. **Distributed GAS execution**
   - Gather phase: local vertices gather from local neighbors; for remote neighbors, send message to remote partition and receive value
   - Apply phase: purely local
   - Scatter phase: send updated values to remote partitions that need them

4. **Distributed ICE**
   - Each partition computes its local diff
   - Neighborhood expansion may cross partition boundaries → requires one round of message passing to identify the full affected subgraph
   - Each partition then runs ICE locally on its affected vertices

### Phase 4: Optimizations (Day 4+)

- LRU eviction of snapshots to disk
- Iteration state sharing in DGSI
- Parallel ICE across multiple snapshots
- Sliding window query optimization

---

## Language Recommendation

**Rust** is the ideal choice:
- Persistent data structures benefit enormously from Rust's ownership model (no GC pauses, predictable memory)
- The `im` crate provides production-quality persistent HAMTs and vectors out of the box
- Excellent networking libraries (tokio) for the distributed layer
- Easy to benchmark and profile

**Go** is a solid alternative if the team is more comfortable with it — simpler concurrency model, though you'd need to implement persistent data structures yourself or use immutable maps.

**Java/Kotlin** on JVM would work but the GC overhead may be problematic for a system that creates many short-lived tree nodes during path-copying.

---

## Key Papers to Read

1. **Tegra** (NSDI 2021) — The main paper. Read sections 3 (Timelapse), 4 (DGSI), and 5 (ICE) carefully.
2. **The Adaptive Radix Tree** (Leis et al., ICDE 2013) — The underlying data structure for DGSI. Understanding ART helps, but you can substitute a HAMT for v1.
3. **Pregel** (SIGMOD 2010) — The foundational vertex-centric model. Tegra's GAS model is a refinement of this.
4. **PowerGraph** (OSDI 2012) — The GAS model that Tegra builds on.

---

## Evaluation Ideas

Once implemented, you can reproduce key results from the paper:

1. **Memory efficiency**: Create N snapshots of a graph with small mutations between them. Measure total memory vs. N × single-snapshot memory. Tegra should be dramatically better due to structural sharing.

2. **Incremental speedup**: Compare ICE (incremental) vs. full recomputation for PageRank/CC on consecutive snapshots with varying mutation rates (1%, 5%, 10%, 50% edges changed).

3. **Ad-hoc window queries**: Run "compute PageRank on 20 consecutive snapshots" and measure total time. Compare with naive serial execution.

4. **Scalability**: Measure throughput as you add more nodes to the distributed system.

Good test datasets:
- Twitter follower graph (available from SNAP)
- LiveJournal social network
- Synthetic power-law graphs (R-MAT generator)

To simulate temporal evolution, start with 80% of edges and add 1% per snapshot (this is what the paper does).

---

## Potential Gotchas

- **Path-copying overhead**: Each mutation copies O(log n) tree nodes. For bulk mutations, batch them and commit once.
- **Cross-partition ICE**: The neighborhood expansion in ICE can cascade across partitions. Set a maximum expansion depth and handle boundary vertices carefully.
- **Snapshot consistency**: All partitions must see the same logical snapshot. The barrier protocol must handle stragglers.
- **Memory fragmentation**: Many small allocations for tree nodes. Consider an arena allocator.
- **Serialization**: For disk eviction, you need to serialize/deserialize ART subtrees efficiently. Consider a flat binary format rather than JSON.
