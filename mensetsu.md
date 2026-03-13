

# Scenarios

- API server and workload heartbeat
- GPU discovery
- IPtables
- Storage Error Code

# Tech

- SSL and XXX
- Leader selection and broker
- partitioning and bootstrapping and data ordering
- Unity Catalog
- relational actions : Actions and Tranformations

# Container and Spark Data Broker Similarity

- Diff in container manifest <-> delta lake fs
- ProtoBuf
    - containerd sock and spark counterpart
- Broker as process separation
- kubeapiserver and zookeeper as controller system
- leader selection in etcd and brokers
    - MetadataRequest for client
        - code

# Unity Catalog (UC)

## Access Control

- GRANT <verb> ON <object> TO <subject>
- REVOKE
- GRANT SELECT ON TABLE catalog.schema.customers TO `data_analysts`;
- verbs
    Verb	Meaning	Applies To
    SELECT	Read data	Table, View, Materialized View
    MODIFY	INSERT, UPDATE, DELETE	Table
    CREATE SCHEMA	Create schemas	Catalog
    CREATE CATALOG	Create catalogs	Metastore
        CREATE VOLUME	Create volumes	Schema
        CREATE FUNCTION	Create UDFs	Schema
        CREATE MODEL	Register ML models	Schema
        CREATE TABLE	Create tables	Schema
    USE CATALOG	Access catalog (traverse)	Catalog
    USE SCHEMA	Access schema (traverse)	Schema
    ALL PRIVILEGES	Everything	Any
    EXECUTE	Run function	Function
    READ FILES	Read raw files	External Location, Volume
    WRITE FILES	Write raw files	External Location, Volume
    READ VOLUME	Read from volume	Volume
    WRITE VOLUME	Write to volume	Volume
    APPLY TAG	Add/edit tags	Table, Schema, Catalog
    MANAGE	Manage grants + ownership	Any securable
- objects
    - kind + object name
    - TABLE prod.sales.customers
- subjects
    - user / group / service principal
    - TO `analysts`

# Lakehouse

## Storage Tiers

Layer           Understands
─────           ───────────
S3              Objects (key-value blob store)
Parquet         Columns + types (file format) ->OLAP
Delta Lake      Tables + ACID + versions (storage layer)
Unity Catalog   Namespaces + access + lineage (governance layer)

## OLTP and OLAP

- Online Transaction / Analytical Processing
- Row-based for continuity / Col-based for compression and analysis

### Delta Lake

- How S3 cheap blob is leveraged by relational UC
- Enforce ACID (Atomic Consistency Isolation Durability) / time travel / schema evolution
- metatdata/ -> stores schema
- parquet file + delta_log/
    - SELECT * FROM silver.transactions VERSION AS OF 2

### ACID Write for Delta Lake

Writer (Spark Driver)
  │
  ├─1─ Read current version from _delta_log/
  │    (e.g., 00000000000000000042.json → version 42)
  │
  ├─2─ Write new Parquet files to data directory
  │    (data files are written FIRST, but invisible — no log entry yet)
  │
  ├─3─ Build commit JSON describing what changed:
  │    {
  │      "add": [{"path": "part-00001.parquet", ...}],
  │      "remove": [{"path": "part-00000.parquet", ...}],
  │      "commitInfo": {"timestamp": ..., "operation": "MERGE"}
  │    }
  │
  ├─4─ Attempt atomic PUT of 00000000000000000043.json
  │    │
  │    ├── SUCCESS → commit is visible, transaction complete
  │    │
  │    └── CONFLICT (file already exists = another writer won)
  │         │
  │         ├── Re-read log to see what changed
  │         ├── Check if conflict is real (do changes touch same files?)
  │         │   ├── No conflict → retry with version 44
  │         │   └── Real conflict → abort, throw ConcurrentModificationException
  │         └── (up to spark.databricks.delta.retryWriteConflict.limit retries)


## Granularity and Lineage and Delta Log

- Lakehouse enforce non-relational into relational for schema in lineage word : catalog.schema.table.colume
- `location` and `format` are key infos
- Managed Table
    - CREATE TABLE catalog.schema.t (...)
- External Table
    - CREATE TABLE catalog.schema.t LOCATION 's3://my-bucket/data/'
- Lineage granularity levels:
    - Table-level:   gold.summary ← silver.txn ← bronze.raw
    - Column-level:  gold.summary.total_amount ← silver.txn.amount ← bronze.raw.amount 
    - Row-level:     gold row #42 came from silver rows #100, #205, #309
    - Cell-level:    gold.summary[row 42].total_amount = silver.txn[row 100].amount + ...

- Unity Catalog doesn't trace data at runtime.
It analyzes the QUERY PLAN (Catalyst logical plan):
  - SQL: SELECT region, SUM(amount) AS total
       FROM silver.txn GROUP BY region
  - Catalyst plan:
    Aggregate [region] [region, sum(amount) AS total]
      └── Scan silver.txn [region, amount]
  - Unity Catalog extracts:
    gold.summary.region      ← silver.txn.region       (direct copy)
    gold.summary.total       ← silver.txn.amount       (aggregation)
  - This is STATIC ANALYSIS of the plan, not runtime tracing.
  - Cost: near zero.

# Lakehouse Monitoring working with Model Serving

# ETL
- Phase
    - Without Delta
    - With Delta Lake
- Extract
    - Ingest → Bronze
    - Append-only raw files; no way to know what's new vs. already ingested	_delta_log tracks what was added; streaming can use Delta as a source to detect new rows automatically
- Transform [Q]
    - Bronze → Silver
    - Must rewrite entire dataset to update or deduplicate rows	MERGE INTO
    - upsert only changed rows (e.g., "If transaction_id exists, update; else insert")
- Load
    - Silver → Gold
    - Overwrite entire table for every aggregation refresh
    - Incremental writes — only re-aggregate new data; old aggregations untouched
- Recovery
    - A bad ETL job permanently corrupts the table
    - Time Travel + RESTORE — roll back to the last known good version in seconds
 
## Checkpointing to speedup long delta_log

- Build checkpoint.parquet to avoid reading historical jsons
- checkpoint.parquet is much faster than json as it's a compressed binary
    Protobuf (Row-oriented, message-focused):
    Message 1: [user_id=1, name="Alice", amount=50]
    Message 2: [user_id=2, name="Bob",   amount=30]
    → Each message is a self-contained unit
    Parquet (Column-oriented, analytics-focused):
    Column "user_id": [1, 2, 3, 4, 5, ...]
    Column "name":    ["Alice", "Bob", "Carol", ...]
    Column "amount":  [50, 30, 20, ...]
    → Data is grouped by column, not by row
- column-oriented parquet has better compression as similar column values compress great

# Shuffle Boundaries, Stages, Tasks, Executors, Asynchronous

## Two Managers Inside Each Executor

    ShuffleManager                  BlockManager
    ┌─────────────────────┐        ┌──────────────────────────┐
    │ "HOW to shuffle"    │        │ "WHERE data lives"       │
    │                     │        │                          │
    │ • Sort & partition  │        │ • MemoryStore (heap/off) │
    │ • Spill to disk     │        │ • DiskStore (local FS)   │
    │ • Write map output  │        │ • Track all blocks       │
    │ • Serve to reducers │        │ • Serve blocks to remote │
    └─────────────────────┘        └──────────────────────────┘
          uses ────────────────────────→ managed by

- ShuffleManager: orchestrates the shuffle process (sort, partition, write, read) — like a controller
- BlockManager: manages storage (memory + disk) where data blocks live — like kubelet's volume manager

## Shuffle Write (Map Side)

    Task → SortShuffleWriter → In-Memory Buffer → [Spill?] → Final Merge → Output Files

1. Records arrive from reading source
2. ShuffleManager creates SortShuffleWriter → uses ExternalSorter
3. ExternalSorter buffers records in memory (PartitionedAppendOnlyMap)
   - sorted by partition + key
4. Memory pressure check via BlockManager.MemoryManager
   - "Do I have enough execution memory to keep buffering?"
5. If memory full → SPILL TO DISK
   - ExternalSorter sorts in-memory data, writes to temp file
   - /tmp/spark-local-xxx/temp_shuffle_spill_0 (first spill)
   - /tmp/spark-local-xxx/temp_shuffle_spill_1 (second spill)
   - Memory buffer cleared → accept more records → repeat
6. FINAL MERGE: all spill files + remaining in-memory → ONE output per executor
   - shuffle_0_0_0.data   ← single sorted file: [partition 0 records][partition 1][partition 2]
   - shuffle_0_0_0.index  ← byte offset of each partition
   - Temp spill files are DELETED

## Unified Memory Pool

    ┌── Unified Memory (60% of (JVM Heap - 300MB)) ──┐
    │                                                  │
    │  Execution Memory     │  Storage Memory          │
    │  (shuffles, sorts,    │  (cached RDDs,           │
    │   joins, aggs)        │   broadcast vars)        │
    │  ◄──── can steal from each other ────►           │
    │  Default split: 50/50 but flexible               │
    └──────────────────────────────────────────────────┘
    User Memory (40%) ← UDFs, objects

- Execution can evict storage if needed
- Storage can borrow execution's share when execution is idle

## Shuffle Read (Reduce Side)

1. TaskScheduler launches reduce task for partition N
2. LOCATE BLOCKS: ask Driver's MapOutputTracker
   - "Where are partition-N chunks from all map tasks?"
   - Driver responds: Map Task 0 → Executor A, Map Task 1 → Executor C, etc.
3. FETCH BLOCKS via BlockTransferService (Netty)
   - Read remote executor's .index file → find byte offset for partition N
   - Read that slice from .data file → send over network
4. Received into BlockManager
   - Small blocks (< 200MB): IN MEMORY
   - Large blocks: SPILL TO DISK
5. Aggregate/process with ExternalAppendOnlyMap or ExternalSorter
   - Same spill mechanism as map side if memory runs out

## Async Aspects of Shuffle

- Map side write: SYNCHRONOUS within task (read → buffer → spill → merge → write)
- Reduce side fetch: ASYNCHRONOUS with pipelining

    ShuffleBlockFetcherIterator
      Fetch Thread Pool (5 concurrent) ──→ fetch from Executor A  (async)
                                       ──→ fetch from Executor B  (async)
                                       ──→ fetch from Executor C  (async)
                         │
                         ↓
                   Receive Buffer (bounded, default 48 MB)
                         │
                         ↓
                   Task Thread consumes (synchronous processing)

- spark.reducer.maxBlocksInFlightPerAddress = 5 (concurrent remote fetches)
- spark.reducer.maxSizeInFlight = 48MB (prefetch buffer)
- Netty I/O layer is async (NIO event loop)

## Manager Collaboration: Write Path

    Task
     └→ ShuffleManager.getWriter()
         └→ SortShuffleWriter
             └→ ExternalSorter
                 ├→ MemoryManager.acquireExecutionMemory()        ← "can I buffer more?"
                 │   └→ BlockManager.MemoryStore evicts if needed
                 ├→ [spill] DiskBlockManager.createTempLocalBlock() ← "give me a temp file"
                 │   └→ BlockManager.DiskStore writes spill
                 └→ IndexShuffleBlockResolver.writeIndexFileAndCommit()
                     └→ Final .data + .index files on disk

## Manager Collaboration: Read Path

    Task
     └→ ShuffleManager.getReader()
         └→ BlockStoreShuffleReader
             └→ MapOutputTracker.getMapSizesByExecutorId()   ← "where are my blocks?"
                 └→ ShuffleBlockFetcherIterator
                     ├→ BlockManager.getLocalBlock()          ← if same executor (no network)
                     └→ BlockTransferService.fetchBlocks()    ← if remote (async Netty)
                         └→ received into BlockManager memory or spilled to disk

## Executor Heartbeats (Two Layers)

    Layer 1 (Resource Manager):
      Worker/Node → Master              "Is this machine alive?"
      Standalone: Worker heartbeat every 15s
      K8s: kubelet node status
      Databricks: managed by control plane

    Layer 2 (Spark Application):
      Executor → HeartbeatReceiver      "Is this JVM process alive?"
      (inside Driver)                    + task metrics / accumulators
      HeartbeatReceiver notifies TaskScheduler about executor liveness
      Timeout: spark.network.timeout = 120s → marks executor lost → reschedule tasks

- Worker alive (Layer 1 OK) but Executor dead (Layer 2 fails) = K8s Node Ready but Pod crash-looping

## Executor Storage: Delta Files vs Shuffle Files

    Executor JVM
    ├── Delta Reader → Hadoop FileSystem API → REMOTE cloud storage (S3/ADLS)
    │   (reads _delta_log + parquet files)        PERMANENT, shared by all executors
    │
    ├── ShuffleManager → Local File I/O → LOCAL disk
    │   (shuffle .data + .index files)            EPHEMERAL, per-executor
    │
    └── BlockManager → Memory + Local Disk
        (cache, spill, received shuffle blocks)   EPHEMERAL

- Delta files use Hadoop FileSystem API (S3AFileSystem, AzureBlobFileSystem, etc.)
- Shuffle files use plain Java local file I/O (java.io.File) — NOT Hadoop FS API
- The two storage paths never share storage and don't know about each other
- Databricks Remote Shuffle Service: moves shuffle files to remote storage (survives executor loss)

## K8s Analogies

    In-memory buffer           ↔  Container memory (RAM)
    Spill to local disk        ↔  emptyDir volume
    maxSizeInFlight (prefetch) ↔  TCP receive window
    Async block fetching       ↔  concurrent HTTP requests from Pod to services
    MapOutputTracker           ↔  CoreDNS ("where is this service?")
    BlockTransferService       ↔  Pod-to-Pod networking
    Shuffle data lost on death ↔  emptyDir lost on Pod eviction
    Remote Shuffle Service     ↔  PersistentVolume (survives Pod death)

# Long-living executor and shuffling consideration

- Shuffling files live locally for speed
- Executors long-lives accross stage to leverage local shuffling files
- Stage level scheduling is thus difficult

# RDD lifecycle

1. DRIVER: Create RDD (lazy — just metadata)
   rdd = sc.textFile(...)      → HadoopRDD object in Driver heap
   rdd2 = rdd.filter(...)      → FilteredRDD object in Driver heap
   rdd3 = rdd2.groupByKey()    → ShuffledRDD object in Driver heap
                                  (still no execution)

2. DRIVER: Action triggers planning
   rdd3.collect()
     → DAGScheduler walks RDD lineage graph
     → Splits at shuffle boundaries → Stages
     → Each Stage × Partition → Task
     → Serializes Task (compute closure + partition info)

3. DRIVER → EXECUTOR: Send serialized Task
   TaskScheduler picks executor with free core
   Sends: "run this function on partition P2"
     (the "function" is the RDD's compute chain, serialized)

4. EXECUTOR: Deserialize and run
   Task.run():
     → Read partition P2 from HDFS/S3
     → Apply filter function
     → Apply partial aggregate
     → Write shuffle output to disk
   
   No RDD object is created. Just function execution on data.

5. EXECUTOR → DRIVER: Return result
   Small result (collect): send rows to Driver
   Shuffle output: write to local disk, report location to Driver

### Transformations = lazy, return a new RDD, no execution. Actions = eager, trigger execution, return a result to Driver or write to storage.


Transformation:  RDD → RDD     (lazy, builds lineage, nothing runs)Action:          RDD → value   (eager, triggers the entire DAG)

# Lakehouse
