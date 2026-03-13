# Data Lakehouse — Comprehensive Solution Reference

## Table of Contents
1. [What Problem It Solves](#1-what-problem-it-solves)
2. [Unified Storage Layer — Delta Lake](#2-unified-storage-layer--delta-lake)
3. [Medallion Architecture](#3-medallion-architecture-bronze--silver--gold)
4. [Data Engineering Solutions](#4-data-engineering-solutions)
5. [SQL Analytics & BI Solutions](#5-sql-analytics--bi-solutions)
6. [Machine Learning Solutions](#6-machine-learning-solutions)
7. [AI / GenAI Solutions](#7-ai--genai-solutions-post-2023)
8. [Governance & Compliance](#8-governance--compliance-solutions)
9. [Delta Sharing](#9-delta-sharing-open-data-exchange)
10. [Architecture Summary](#10-architecture-summary)

---

## 1. What Problem It Solves

Traditional data architectures forced organizations into one of two unsatisfactory positions:

### The Old Trade-off

| Dimension | Data Warehouse | Data Lake |
|---|---|---|
| Data types | Structured only | All: structured, semi, unstructured |
| Storage cost | High (proprietary) | Low (object storage) |
| Schema model | Schema-on-write (rigid) | Schema-on-read (flexible) |
| Consistency | ACID transactions | No ACID (eventual consistency) |
| BI / SQL performance | Excellent | Poor without tuning |
| ML / AI workloads | Not designed for | Native (files, notebooks) |
| Governance | Centralized (built-in) | Fragmented / bolt-on |
| Real-time ingestion | Limited | Supported but complex |
| Data quality | Enforced at ingest | Often unmanaged |

### What Lakehouse Unifies

The Lakehouse pattern combines:
- **Cheap object storage** (like a data lake)
- **ACID transactions + schema enforcement** (like a warehouse)
- **Unified compute for BI, ETL, ML** (eliminating data copies)
- **Single governance layer** across all workloads

---

## 2. Unified Storage Layer — Delta Lake

Delta Lake is an **open-source storage framework** that runs on top of cloud object storage (Amazon S3, Azure Data Lake Storage Gen2, Google Cloud Storage) and adds a transaction log (`_delta_log/`) that turns a folder of Parquet files into a reliable, queryable table.

### 2.1 ACID Transactions

Delta Lake implements **serializable isolation** for concurrent reads and writes:

- **Atomicity**: A write either fully succeeds or is fully rolled back
- **Consistency**: Schema constraints are enforced before any data lands
- **Isolation**: Concurrent writers use optimistic concurrency control — the first writer to commit wins; the second retries automatically
- **Durability**: Once committed to the log, data is permanent

```python
# Concurrent writers are safe — no corruption
spark.sql("INSERT INTO orders SELECT * FROM new_orders")  # Writer 1
spark.sql("UPDATE orders SET status='closed' WHERE id=99")  # Writer 2 — safe
```

**Business value**: Eliminates the need for external locking mechanisms or maintenance windows for writes.

### 2.2 Schema Enforcement

Delta rejects writes that don't match the table schema at write time:

```python
# This will FAIL if 'amount' is defined as LongType but string is passed
df_bad = spark.createDataFrame([("abc",)], ["amount"])
df_bad.write.format("delta").mode("append").save("/delta/orders")
# AnalysisException: incompatible data type
```

**Business value**: Prevents silent data corruption from upstream schema changes.

### 2.3 Schema Evolution

When intentional schema changes are needed, Delta supports `mergeSchema`:

```python
df_with_new_column.write \
  .format("delta") \
  .option("mergeSchema", "true") \
  .mode("append") \
  .save("/delta/orders")
```

Supported evolutions:
- Adding new nullable columns
- Widening numeric types (e.g., `INT` → `LONG`)
- Adding new nested fields

**Business value**: Upstream producers can add fields without breaking downstream consumers.

### 2.4 Time Travel (Data Versioning)

Every write to a Delta table creates a new version. Historical versions are queryable:

```python
# Query by version number
df = spark.read.format("delta").option("versionAsOf", 5).load("/delta/orders")

# Query by timestamp
df = spark.read.format("delta") \
  .option("timestampAsOf", "2025-01-01 00:00:00") \
  .load("/delta/orders")

# SQL syntax
spark.sql("SELECT * FROM orders VERSION AS OF 5")
spark.sql("SELECT * FROM orders TIMESTAMP AS OF '2025-01-01'")
```

Retention is configurable:
```python
spark.sql("""
  ALTER TABLE orders
  SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 90 days')
""")
```

**Business value**: Reproducible ML training datasets, audit compliance, accidental-delete recovery without backup restores.

### 2.5 MERGE INTO (Upserts)

`MERGE` is the most powerful Delta operation — critical for CDC (Change Data Capture) and SCD (Slowly Changing Dimensions):

```sql
MERGE INTO silver_customers AS target
USING cdc_events AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.operation = 'UPDATE' THEN
  UPDATE SET *
WHEN MATCHED AND source.operation = 'DELETE' THEN
  DELETE
WHEN NOT MATCHED THEN
  INSERT *
```

**Business value**: Enables real-time data lake updates from transactional systems (CDC pipelines from PostgreSQL, MySQL, SQL Server via Debezium/DMS).

### 2.6 File Optimization

#### OPTIMIZE + Z-ORDER
```sql
-- Compact small files and cluster by most-queried columns
OPTIMIZE orders ZORDER BY (customer_id, order_date)
```

- **OPTIMIZE**: Merges many small Parquet files into fewer large ones (eliminates "small file problem")
- **Z-ORDER**: Multi-dimensional data skipping — stores similar values physically close together. A query on `customer_id = 12345` may skip 90%+ of files entirely

#### Auto Optimize (Databricks-managed)
```sql
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
```

**Business value**: Eliminates manual maintenance jobs; query performance degrades gracefully instead of catastrophically as data grows.

### 2.7 Deletion Vectors

Row-level soft deletes that avoid full Parquet file rewrites:

- A deletion vector file marks specific rows as deleted
- Reads filter out marked rows transparently
- A later `VACUUM` or `OPTIMIZE` physically removes them

**Business value**: GDPR right-to-erasure requests can be processed in seconds instead of hours (no file rewrite until next maintenance window).

### 2.8 Liquid Clustering (Delta 3.x)

Replaces static partition columns with dynamic clustering:

```sql
ALTER TABLE orders CLUSTER BY (customer_id, region)
```

- No partition skew issues from high-cardinality columns
- Clustering evolves with query patterns without table rewrites
- Incremental clustering — only newly written data is clustered immediately

---

## 3. Medallion Architecture (Bronze → Silver → Gold)

A three-layer data quality pattern that separates concerns: ingestion, quality, and business value.

```
External Sources
      │
      ▼
┌──────────┐     ┌──────────┐     ┌──────────┐
│  BRONZE  │────▶│  SILVER  │────▶│   GOLD   │
│  (Raw)   │     │ (Cleaned)│     │(Business)│
└──────────┘     └──────────┘     └──────────┘
      │                │                │
  Data Eng         Analytics         ML / BI
  Pipelines         Teams             Apps
```

### 3.1 Bronze Layer — Raw Ingestion

**Purpose**: Land all raw data exactly as received, immutably.

Characteristics:
- Append-only (never update)
- Schema-on-read or minimal schema
- All fields preserved, including corrupt records
- Metadata columns added: `ingestion_timestamp`, `source_system`, `source_file`

```python
from pyspark.sql.functions import current_timestamp, lit

df_raw = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load("abfss://landing@storage.dfs.core.windows.net/orders/")

df_bronze = df_raw \
    .withColumn("ingestion_ts", current_timestamp()) \
    .withColumn("source_system", lit("orders-api-v2"))

df_bronze.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze_orders") \
    .outputMode("append") \
    .toTable("bronze.orders_raw")
```

**Business value**: Complete audit trail; any downstream bug can be fixed by reprocessing from Bronze — no data is ever lost.

### 3.2 Silver Layer — Cleaned & Conformed

**Purpose**: Apply business rules, clean data, and establish relationships.

Transformations applied:
- Null handling and default values
- Type casting and format normalization
- Deduplication
- PII masking or tokenization
- Business key resolution (e.g., `customer_id` lookup)
- Data quality rule application

```python
from pyspark.sql.functions import col, when, to_date

df_silver = spark.readStream.table("bronze.orders_raw") \
    .filter(col("order_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .withColumn("order_date", to_date(col("order_date_str"), "yyyy-MM-dd")) \
    .withColumn("amount_usd",
        when(col("currency") == "EUR", col("amount") * 1.08)
        .otherwise(col("amount"))) \
    .dropDuplicates(["order_id"]) \
    .select("order_id", "customer_id", "order_date", "amount_usd", "status")

df_silver.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/silver_orders") \
    .outputMode("append") \
    .toTable("silver.orders_clean")
```

**Business value**: Single source of truth for analysts; data scientists and analysts work from the same clean dataset without each building their own cleaning logic.

### 3.3 Gold Layer — Business Aggregations

**Purpose**: KPI-level aggregations optimized for specific business questions.

```python
from pyspark.sql.functions import sum, count, avg, date_trunc

df_gold = spark.table("silver.orders_clean") \
    .groupBy(
        date_trunc("day", col("order_date")).alias("date"),
        col("region")
    ) \
    .agg(
        sum("amount_usd").alias("total_revenue"),
        count("order_id").alias("order_count"),
        avg("amount_usd").alias("avg_order_value")
    )

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.daily_revenue_by_region")
```

Gold tables are typically:
- Refreshed on a schedule (hourly, daily)
- Pre-joined for common query patterns
- Exposed directly to BI tools or API layers

**Business value**: Sub-second BI queries; business users self-serve without burdening data engineers.

---

## 4. Data Engineering Solutions

### 4.1 Structured Streaming — Real-Time Ingestion

Spark Structured Streaming processes data as it arrives with exactly-once semantics:

```python
# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payload
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

schema = StructType() \
    .add("txn_id", StringType()) \
    .add("user_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("event_time", TimestampType())

parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Windowed aggregation with watermarking
from pyspark.sql.functions import window, sum

agg_df = parsed_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "5 minutes"), col("user_id")) \
    .agg(sum("amount").alias("total_amount"))

# Write to Delta
agg_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/txn_stream") \
    .toTable("silver.txn_5min_agg")
```

**Trigger modes**:
| Mode | Syntax | Behavior |
|---|---|---|
| Micro-batch | `Trigger.ProcessingTime("30 seconds")` | Process every 30s |
| Once | `Trigger.Once()` | Run once and stop (scheduled jobs) |
| Available Now | `Trigger.AvailableNow()` | Process all available, then stop |
| Continuous | `Trigger.Continuous("1 second")` | True continuous (~10ms latency, experimental) |

**Business value**: Eliminates the Lambda architecture — no need for a separate "speed layer" (Flink/Storm) + "batch layer" (Spark) — one engine handles both.

### 4.2 Auto Loader — Cloud File Ingestion

Auto Loader uses cloud storage event notifications to detect new files incrementally:

```python
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/schemas/orders") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("abfss://raw@storage.dfs.core.windows.net/orders/")
```

Features:
- **Schema inference**: Samples files to detect schema automatically
- **Schema evolution**: New columns added automatically with `addNewColumns` mode
- **Exactly-once**: Checkpoint tracks exactly which files have been processed
- **Scalability**: Uses cloud notification queues (SQS/Event Grid) — no directory listing at scale
- **Rescue data**: Corrupt records saved to `_rescued_data` column instead of being dropped

**Auto Loader notification modes**:
| Mode | How it works | Scale |
|---|---|---|
| Directory listing | Lists storage directory periodically | Small–medium |
| File notification | Cloud event → queue → Spark | Billions of files |

**Business value**: Replaces brittle cron + `COPY INTO` patterns. Handles backfills, late-arriving files, and schema drift automatically.

### 4.3 Delta Live Tables (DLT) — Declarative Pipelines

DLT is a framework that manages the lifecycle, quality, and dependencies of Lakehouse pipelines:

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

# Bronze: raw ingestion
@dlt.table(
    comment="Raw orders from the API",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/landing/orders/")

# Silver: cleaned with quality expectations
@dlt.table(
    comment="Validated and cleaned orders",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect("valid_status", "status IN ('open', 'closed', 'cancelled')")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .withColumn("processed_at", current_timestamp())
        .withColumn("amount_usd", col("amount").cast("double"))
    )

# Gold: aggregated KPIs
@dlt.table(
    comment="Daily revenue KPIs by region",
    table_properties={"quality": "gold"}
)
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")
        .groupBy("order_date", "region")
        .agg({"amount_usd": "sum", "order_id": "count"})
    )
```

**DLT quality modes**:
| Decorator | Behavior on violation |
|---|---|
| `@dlt.expect` | Log warning, keep row |
| `@dlt.expect_or_drop` | Drop violating row, log metric |
| `@dlt.expect_or_fail` | Stop entire pipeline immediately |

**DLT pipeline modes**:
| Mode | Behavior |
|---|---|
| Triggered | Runs on schedule or manually |
| Continuous | Runs indefinitely as a streaming job |

**DLT advanced features**:
- **Materialized views**: Incrementally maintained aggregations
- **Streaming tables**: Append-only targets for real-time data
- **Change Data Feed consumption**: Built-in `apply_changes()` for CDC workloads

```python
# CDC with apply_changes
dlt.apply_changes(
    target="silver_customers",
    source="bronze_cdc_customers",
    keys=["customer_id"],
    sequence_by="event_timestamp",
    apply_as_deletes=expr("operation = 'DELETE'"),
    except_column_list=["operation", "event_timestamp"]
)
```

**Business value**: Self-documenting pipelines; data quality metrics visible in the UI; dependency graph auto-computed; no manual orchestration of table dependencies.

### 4.4 Apache Kafka Integration

Common Databricks + Kafka patterns:

#### Read from Kafka → Write to Delta
```python
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "orders,payments,shipments") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .load()
```

#### Delta Change Data Feed → Kafka (reverse sync)
```python
# Publish Delta table changes to Kafka for downstream consumers
changes = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("silver.orders")

changes.selectExpr(
    "CAST(order_id AS STRING) AS key",
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "orders-cdc") \
    .start()
```

#### Exactly-once write to Kafka
```python
df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "processed-orders") \
    .option("kafka.transactional.id", "databricks-producer-001") \
    .option("kafka.enable.idempotence", "true") \
    .start()
```

**Business value**: Bidirectional sync between the Lakehouse and event-driven microservices.

### 4.5 Orchestration — Databricks Workflows

Workflows manage multi-task DAGs with built-in retry, alerting, and dependency tracking:

| Feature | Detail |
|---|---|
| Task types | Notebook, Python script, DLT pipeline, SQL, dbt, JAR |
| Dependencies | Sequential, parallel, conditional (`if/else`) |
| Parameters | Job-level and task-level parameterization |
| Retry policy | Per-task retry count and backoff |
| Alerts | Email, Slack, PagerDuty on failure |
| Scheduling | Cron, file arrival, continuous |
| Repair runs | Re-run only failed tasks without repeating successful ones |

---

## 5. SQL Analytics & BI Solutions

### 5.1 Databricks SQL Warehouse

Databricks SQL provides dedicated compute clusters for SQL analytics:

**Warehouse types**:
| Type | Use case | Cold start | Photon |
|---|---|---|---|
| Classic | Predictable workloads | ~2 min | Optional |
| Pro | Complex queries, concurrency | ~2 min | Included |
| Serverless | Ad-hoc, cost-sensitive | ~5 sec | Included |

**Photon Engine**: A vectorized C++ query engine that runs Spark SQL:
- Columnar processing with SIMD instructions
- Eliminates JVM overhead for SQL operations
- 2–12x faster than standard Spark SQL on analytical queries
- Fully transparent — no code changes required
- Particularly effective for: `JOIN`, `GROUP BY`, `WINDOW`, JSON/string operations

**Serverless SQL Warehouse advantages**:
- Instant (5-second) startup
- Auto-scales from 0 to handle burst queries
- Per-query billing (no idle cost)
- Managed by Databricks (no cluster configuration)

### 5.2 BI Tool Integration

```
Power BI ─────────┐
Tableau ──────────┤
Looker ───────────┼──▶ JDBC/ODBC ──▶ SQL Warehouse ──▶ Delta Lake
Qlik ─────────────┤
Excel (Partner BI)┘
```

Connection methods:
| BI Tool | Connector | Protocol |
|---|---|---|
| Power BI | Native Databricks connector | HTTP/Arrow |
| Tableau | Databricks JDBC driver | JDBC |
| Looker | Databricks SQL dialect | JDBC |
| dbt | dbt-databricks adapter | JDBC/HTTP |
| Excel | ODBC or Partner Connect | ODBC |

**Databricks SQL features for analysts**:
- SQL Editor with auto-complete and schema browser
- Dashboards with scheduled refreshes
- Alerts on data thresholds (e.g., "notify when daily orders < 100")
- Query history and performance profiling

**Business value**: Analysts use familiar tools (Power BI, Tableau) against fresh, governed data without any copy to a separate warehouse.

### 5.3 Unity Catalog — Governance Layer

Unity Catalog is the **metastore and governance control plane** for all Databricks assets:

#### 3-Level Namespace
```sql
-- Fully qualified reference
SELECT * FROM main_catalog.sales_schema.orders_table

-- 3 levels:
-- main_catalog  = organizational boundary (e.g., "prod", "dev", "finance")
-- sales_schema  = team or domain boundary
-- orders_table  = individual asset
```

#### Securable Objects Hierarchy
```
Metastore (one per region)
└── Catalog
    └── Schema
        ├── Table / View / Materialized View
        ├── Volume (unstructured files)
        ├── Function (UDFs, ML models)
        ├── Connection (external data sources)
        └── External Location (cloud storage path)
```

#### Fine-Grained Access Control
```sql
-- Table-level
GRANT SELECT ON TABLE sales.orders TO `analyst_team`;
GRANT MODIFY ON TABLE sales.orders TO `etl_service_principal`;

-- Column-level access
GRANT SELECT (order_id, order_date, region) ON TABLE sales.orders TO `analyst_team`;

-- Schema-level
GRANT USE SCHEMA ON SCHEMA sales TO `all_users`;
GRANT CREATE TABLE ON SCHEMA sales TO `data_engineers`;

-- Catalog-level
GRANT USE CATALOG ON CATALOG prod TO `all_employees`;
```

#### Row-Level Security
```sql
-- Create a row filter function
CREATE FUNCTION sales.region_filter(region STRING)
  RETURN IF(
    is_account_group_member('global_analysts'), true,
    region = current_user_attribute('home_region')
  );

-- Apply to table
ALTER TABLE sales.orders SET ROW FILTER sales.region_filter ON (region);
-- Now each user only sees rows matching their home region
```

#### Column Masking
```sql
-- Create a masking function
CREATE FUNCTION sales.mask_email(email STRING)
  RETURN CASE
    WHEN is_account_group_member('pii_access') THEN email
    ELSE regexp_replace(email, '(.+)@', '***@')
  END;

-- Apply to column
ALTER TABLE customers ALTER COLUMN email SET MASK sales.mask_email;
-- Users without 'pii_access' see: ***@example.com
```

#### Data Lineage (Automatic)
Unity Catalog automatically tracks:
- Which table was read to produce another
- Which notebook/job performed the transformation
- Which model was trained on which table
- Which dashboard queries which table
- Column-level lineage (which source columns feed which target columns)

```python
# Query lineage via API
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Table-level lineage
lineage = w.lineage_tracking.get_table_lineage(
    table_name="main.gold.daily_revenue"
)
for upstream in lineage.upstream_tables:
    print(f"Depends on: {upstream.name}")

# Column-level lineage
col_lineage = w.lineage_tracking.get_column_lineage(
    table_name="main.gold.daily_revenue",
    column_name="total_revenue"
)
```

**Business value**: One set of permissions governs BI, ML, notebooks, and APIs — no per-product access management. Lineage enables impact analysis before any schema change.

---

## 6. Machine Learning Solutions

### 6.1 Feature Store

The Databricks Feature Store solves two critical ML problems:
1. **Feature reuse** — compute once, use in many models
2. **Training-serving skew** — guarantee training features match serving features

#### Creating and Writing Features
```python
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Create a feature table backed by Delta
fe.create_table(
    name="main.ml_features.customer_features",
    primary_keys=["customer_id"],
    timestamp_keys=["feature_date"],  # enables point-in-time lookup
    description="Customer behavioral features for churn model",
    schema=customer_features_df.schema
)

# Write features (supports merge, overwrite, append)
fe.write_table(
    name="main.ml_features.customer_features",
    df=customer_features_df,
    mode="merge"  # upsert on primary key
)
```

#### Point-in-Time Correct Training Sets
```python
from databricks.feature_engineering import FeatureLookup

# Labels: (customer_id, label_date, churned)
# Features are looked up AS OF label_date — no future data leakage
training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(
            table_name="main.ml_features.customer_features",
            lookup_key="customer_id",
            timestamp_lookup_key="label_date",
            feature_names=[
                "lifetime_value",
                "days_since_last_order",
                "order_count_30d",
                "avg_order_value_90d",
                "churn_risk_score"
            ]
        ),
        FeatureLookup(
            table_name="main.ml_features.product_engagement",
            lookup_key="customer_id",
            timestamp_lookup_key="label_date",
            feature_names=["page_views_7d", "cart_abandonment_rate"]
        )
    ],
    label="churned"
)

training_df = training_set.load_df()
```

#### Online Feature Serving
```python
# Publish features to online store for low-latency inference
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Create online table from offline feature table
fe.publish_table(
    name="main.ml_features.customer_features",
    online_table_name="main.ml_features.customer_features_online",
    mode="SNAPSHOT_AND_INCREMENTAL"  # syncs incrementally
)

# At inference time — score batch with automatic feature lookup
predictions = fe.score_batch(
    model_uri="models:/main.ml_models.churn_model@champion",
    df=batch_df  # only needs primary key column(s)
)
```

**Business value**: Eliminates 30–60% of ML project time spent on ad-hoc feature engineering. Prevents model accuracy degradation from inconsistent feature computation between training and serving.

### 6.2 MLflow — Experiment Tracking & Registry

#### Experiment Tracking
```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, f1_score

mlflow.set_experiment("/Users/analyst@company.com/churn-model-v2")

with mlflow.start_run(run_name="GBM-tuned-lr0.05"):
    # Log parameters
    params = {
        "n_estimators": 200,
        "learning_rate": 0.05,
        "max_depth": 4,
        "subsample": 0.8,
        "min_samples_leaf": 10
    }
    mlflow.log_params(params)

    # Train
    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    # Log metrics
    train_auc = roc_auc_score(y_train, model.predict_proba(X_train)[:, 1])
    val_auc = roc_auc_score(y_val, model.predict_proba(X_val)[:, 1])
    val_f1 = f1_score(y_val, model.predict(X_val))

    mlflow.log_metrics({
        "train_auc": train_auc,
        "val_auc": val_auc,
        "val_f1": val_f1,
        "train_val_gap": train_auc - val_auc  # overfitting indicator
    })

    # Log artifacts
    mlflow.log_artifact("feature_importance.png")
    mlflow.log_artifact("confusion_matrix.png")

    # Log model with signature and input example
    signature = mlflow.models.infer_signature(X_train, model.predict(X_train))
    mlflow.sklearn.log_model(
        model,
        name="churn_gbm",
        signature=signature,
        input_example=X_train.iloc[:3]
    )
```

#### Model Registry Lifecycle
```python
from mlflow import MlflowClient

client = MlflowClient()

# Register model in Unity Catalog
mv = mlflow.register_model(
    model_uri=f"runs:/{run_id}/churn_gbm",
    name="main.ml_models.churn_predictor"
)

# Set aliases for lifecycle stages
client.set_registered_model_alias(
    name="main.ml_models.churn_predictor",
    alias="challenger",
    version=mv.version
)

# After validation, promote to champion
client.set_registered_model_alias(
    name="main.ml_models.churn_predictor",
    alias="champion",
    version=mv.version
)

# Load model by alias — always gets the latest champion
model = mlflow.pyfunc.load_model(
    "models:/main.ml_models.churn_predictor@champion"
)
```

#### Model version comparison
```python
# Compare runs programmatically
runs = mlflow.search_runs(
    experiment_ids=["12345"],
    filter_string="metrics.val_auc > 0.85",
    order_by=["metrics.val_auc DESC"],
    max_results=5
)
print(runs[["run_id", "params.n_estimators", "metrics.val_auc", "metrics.val_f1"]])
```

**Business value**: Reproducibility (every experiment is tracked), auditability (governance on model versions), and safe promotion (alias-based deployment, not version numbers).

### 6.3 AutoML

Databricks AutoML automates the initial model building process:

```python
from databricks import automl

summary = automl.classify(
    dataset=training_df,
    target_col="churned",
    primary_metric="roc_auc",
    timeout_minutes=30,
    max_trials=50
)

# Access the best model
print(f"Best trial: {summary.best_trial}")
print(f"Best AUC: {summary.best_trial.metrics['test_roc_auc']}")

# AutoML generates editable notebooks for each trial
print(f"Notebook: {summary.best_trial.notebook_path}")
```

What AutoML does:
- Automatic feature engineering (one-hot encoding, imputation, scaling)
- Trains multiple algorithm families (XGBoost, LightGBM, sklearn)
- Hyperparameter tuning via Hyperopt
- Generates a readable Python notebook for each trial
- Logs all runs to MLflow

**Business value**: Accelerates baseline model creation. The generated notebooks are editable — not a black box.

### 6.4 Model Serving (Mosaic AI)

#### Deploy as REST Endpoint
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedModelInput, TrafficConfig, Route
)

w = WorkspaceClient()

# Create endpoint with A/B traffic splitting
endpoint = w.serving_endpoints.create(
    name="churn-predictor-prod",
    config=EndpointCoreConfigInput(
        served_models=[
            ServedModelInput(
                name="champion",
                model_name="main.ml_models.churn_predictor",
                model_version="3",
                scale_to_zero_enabled=True,
                workload_size="Small"  # Small / Medium / Large
            ),
            ServedModelInput(
                name="challenger",
                model_name="main.ml_models.churn_predictor",
                model_version="4",
                scale_to_zero_enabled=True,
                workload_size="Small"
            )
        ],
        traffic_config=TrafficConfig(
            routes=[
                Route(served_model_name="champion", traffic_percentage=90),
                Route(served_model_name="challenger", traffic_percentage=10)
            ]
        )
    )
)
```

#### Call the Endpoint
```bash
curl -X POST \
  "https://<workspace>.azuredatabricks.net/serving-endpoints/churn-predictor-prod/invocations" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "dataframe_records": [
      {"customer_id": "C001", "days_since_order": 45, "ltv": 230.5},
      {"customer_id": "C002", "days_since_order": 3, "ltv": 1450.0}
    ]
  }'
```

```python
# Python client
import requests

response = requests.post(
    f"https://{workspace_url}/serving-endpoints/churn-predictor-prod/invocations",
    headers={"Authorization": f"Bearer {token}"},
    json={"dataframe_records": [{"customer_id": "C001", "days_since_order": 45}]}
)
print(response.json())
```

**Model serving features**:
| Feature | Detail |
|---|---|
| Autoscaling | Scale from 0 to N based on QPS |
| GPU support | For LLMs and deep learning models |
| A/B testing | Traffic splitting between versions |
| Auth | Databricks token (same as workspace) |
| Monitoring | Inference logs to Delta table |
| Latency | P50 < 20ms for small models |

---

## 7. AI / GenAI Solutions (Post-2023)

### 7.1 Foundation Model APIs

Databricks hosts open-source foundation models as pay-per-token serverless endpoints:

| Model | Provider | Parameters | Use Case |
|---|---|---|---|
| DBRX Instruct | Databricks | 132B (MoE) | General instruction following |
| Meta Llama 3.1 405B | Meta | 405B | Highest quality reasoning |
| Meta Llama 3.1 70B | Meta | 70B | Balanced quality + speed |
| Meta Llama 3.1 8B | Meta | 8B | Fast, cost-efficient |
| Mixtral 8x7B Instruct | Mistral AI | 46.7B (MoE) | Cost-efficient general |
| BGE-Large-EN | BAAI | 335M | Text embeddings |
| GTE-Large | Alibaba | 335M | Text embeddings |

```python
import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")

# Chat completion
response = client.predict(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    inputs={
        "messages": [
            {"role": "system", "content": "You are a helpful data analyst."},
            {"role": "user", "content": "Summarize Q3 revenue trends from the attached data."}
        ],
        "max_tokens": 512,
        "temperature": 0.1
    }
)
print(response["choices"][0]["message"]["content"])

# Embeddings
embedding_response = client.predict(
    endpoint="databricks-bge-large-en",
    inputs={
        "input": ["How do I configure SSO?", "What is the refund policy?"]
    }
)
vectors = embedding_response["data"]  # list of embedding vectors
```

**Business value**: No GPU provisioning or model hosting. Data stays within the Databricks VPC. Subject to the same Unity Catalog audit logging as all other data assets. Pay-per-token — no idle GPU cost.

### 7.2 Vector Search — Managed RAG Infrastructure

Vector Search provides a managed vector database that **auto-syncs with Delta tables**:

#### Create a Vector Search Index
```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# Create a vector search endpoint (compute)
vsc.create_endpoint(
    name="vs-endpoint-prod",
    endpoint_type="STANDARD"
)

# Create index that auto-syncs from a Delta table
vsc.create_delta_sync_index(
    endpoint_name="vs-endpoint-prod",
    index_name="main.rag.product_docs_index",
    source_table_name="main.rag.product_documents",
    pipeline_type="TRIGGERED",           # or CONTINUOUS
    primary_key="doc_id",
    embedding_source_column="content",   # column to embed
    embedding_model_endpoint_name="databricks-bge-large-en"
)
```

#### Query the Index
```python
results = vsc.get_index(
    endpoint_name="vs-endpoint-prod",
    index_name="main.rag.product_docs_index"
).similarity_search(
    query_text="How do I configure SSO with Okta?",
    num_results=5,
    filters={"product_line": "Enterprise"},
    columns=["doc_id", "content", "title", "url"]
)

for doc in results["result"]["data_array"]:
    print(f"Score: {doc[-1]:.3f} | {doc[2]}")
```

#### Full RAG Pipeline with LangChain
```python
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain_community.chat_models import ChatDatabricks
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate

# Vector store retriever
vectorstore = DatabricksVectorSearch(
    index=vsc.get_index("vs-endpoint-prod", "main.rag.product_docs_index"),
    text_column="content",
    columns=["title", "url"]
)
retriever = vectorstore.as_retriever(search_kwargs={"k": 5})

# LLM
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-70b-instruct",
    max_tokens=1024,
    temperature=0.1
)

# Prompt template
prompt = PromptTemplate(
    template="""Answer the question based on the context below.
If the answer is not in the context, say "I don't have enough information."

Context: {context}

Question: {question}

Answer:""",
    input_variables=["context", "question"]
)

# Chain
qa_chain = RetrievalQA.from_chain_type(
    llm=llm,
    retriever=retriever,
    return_source_documents=True,
    chain_type_kwargs={"prompt": prompt}
)

result = qa_chain.invoke({"query": "What are the data retention policies?"})
print(result["result"])
for doc in result["source_documents"]:
    print(f"  Source: {doc.metadata['title']} — {doc.metadata['url']}")
```

**Business value**: No external vector database dependency (Pinecone, Weaviate, etc.). Embeddings and documents governed under Unity Catalog. Auto-sync means the RAG knowledge base stays fresh as source Delta tables update — no manual re-indexing pipeline.

### 7.3 LLM Fine-Tuning (Mosaic AI Training)

Fine-tune open models on proprietary data:

```python
from databricks.model_training import foundation_model as fm

# Prepare training data in JSONL format on a UC Volume
# Format: {"messages": [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]}

run = fm.create(
    model="meta-llama/Meta-Llama-3.1-8B-Instruct",
    train_data_path="dbfs:/Volumes/main/training/data/support_tickets.jsonl",
    eval_data_path="dbfs:/Volumes/main/training/data/support_tickets_eval.jsonl",
    register_to="main.ml_models.support_llm",
    training_duration="2ep",         # 2 epochs
    learning_rate=5e-6,
    context_length=4096,
    data_prep_cluster_id="<cluster-id>"
)

# Monitor training
events = run.get_events()
for event in events:
    print(f"[{event.timestamp}] {event.type}: {event.message}")
```

**Fine-tuning methods supported**:
| Method | Use case | GPU requirement |
|---|---|---|
| Full fine-tuning | Maximum quality | Multi-GPU (A100s) |
| LoRA (Low-Rank Adaptation) | Efficient tuning, less data | Single GPU |
| Continued pre-training | Domain vocabulary expansion | Multi-GPU |

**Business value**: Custom LLMs trained on internal knowledge. Data never leaves the Databricks environment. The fine-tuned model registers directly to Unity Catalog's model registry.

### 7.4 AI Playground

Interactive chat interface for testing foundation models and fine-tuned models side-by-side:

- Compare outputs from multiple models simultaneously
- Adjust temperature, max_tokens, system prompt in real time
- Test with your custom fine-tuned endpoints
- Export working configurations as Python code

---

## 8. Governance & Compliance Solutions

### 8.1 GDPR Right-to-Erasure

```sql
-- Step 1: Identify all records for the data subject
SELECT table_name, count(*)
FROM (
  SELECT 'customers' AS table_name FROM silver.customers WHERE email = 'user@example.com'
  UNION ALL
  SELECT 'orders' FROM silver.orders WHERE customer_id = 'C12345'
  UNION ALL
  SELECT 'interactions' FROM silver.interactions WHERE user_email = 'user@example.com'
);

-- Step 2: Delete records (Deletion Vectors — near-instant)
DELETE FROM silver.customers WHERE email = 'user@example.com';
DELETE FROM silver.orders WHERE customer_id = 'C12345';
DELETE FROM silver.interactions WHERE user_email = 'user@example.com';

-- Step 3: Verify deletion in current version
SELECT COUNT(*) FROM silver.customers WHERE email = 'user@example.com'; -- 0

-- Step 4: Remove from historical versions (physical erasure)
-- After retention window expires, or force immediate:
VACUUM silver.customers RETAIN 0 HOURS;
VACUUM silver.orders RETAIN 0 HOURS;
```

### 8.2 Data Residency Control

```sql
-- Create a catalog pinned to EU storage
CREATE CATALOG eu_data
  MANAGED LOCATION 'abfss://eu-container@eustorage.dfs.core.windows.net/';

-- All tables created here physically reside in the EU region
CREATE TABLE eu_data.gdpr_scope.customer_profiles (
  customer_id STRING,
  name STRING,
  email STRING,
  country STRING
);

-- US catalog for US-only data
CREATE CATALOG us_data
  MANAGED LOCATION 's3://us-east-1-bucket/us-data/';
```

### 8.3 Audit Logging

All Unity Catalog operations generate audit events available in system tables:

```sql
-- Who accessed PII tables in the last 7 days?
SELECT
    user_identity.email AS user,
    request_params.full_name_arg AS table_accessed,
    action_name,
    event_time,
    source_ip_address
FROM system.access.audit
WHERE action_name IN ('getTable', 'commandSubmit')
  AND request_params.full_name_arg LIKE 'main.gdpr%'
  AND event_time > current_timestamp() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- Which service principals modified production tables?
SELECT
    user_identity.email AS principal,
    request_params.full_name_arg AS table_modified,
    action_name,
    event_time
FROM system.access.audit
WHERE action_name IN ('createTable', 'alterTable', 'deleteTable')
  AND request_params.full_name_arg LIKE 'prod.%'
  AND event_time > current_timestamp() - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### 8.4 System Tables for Observability

Databricks system tables provide operational intelligence:

| System Table | What it tracks |
|---|---|
| `system.access.audit` | All UC operations (reads, writes, grants) |
| `system.billing.usage` | DBU consumption per workspace/cluster |
| `system.compute.clusters` | Cluster lifecycle events |
| `system.storage.predictive_optimization` | Auto-optimization recommendations |
| `system.lakeflow.pipeline_events` | DLT pipeline events |

```sql
-- Cost attribution by team
SELECT
    usage_metadata.cluster_id,
    usage_metadata.job_id,
    workspace_id,
    SUM(usage_quantity) AS total_dbu,
    SUM(usage_quantity * list_price) AS estimated_cost_usd
FROM system.billing.usage
WHERE usage_date >= '2025-01-01'
GROUP BY ALL
ORDER BY estimated_cost_usd DESC
LIMIT 20;
```

### 8.5 Compliance Summary

| Regulation | Lakehouse Capability |
|---|---|
| GDPR Art. 17 (erasure) | Delta `DELETE` + `VACUUM` |
| GDPR Art. 20 (portability) | Delta Sharing (open protocol export) |
| GDPR Art. 30 (records of processing) | Unity Catalog lineage |
| GDPR Art. 35 (impact assessment) | System table audit logs |
| HIPAA audit trail | `system.access.audit` → SIEM |
| SOC 2 Type II | UC access control + audit logs |
| PCI DSS column masking | UC column masks on card numbers |
| CCPA opt-out | Row filter + deletion workflow |
| Data residency (EU/US/APAC) | Per-catalog storage location pinning |

---

## 9. Delta Sharing — Open Data Exchange

Delta Sharing is an **open protocol** for sharing live data across organizations without copying it.

### Architecture

```
 Data Provider (Databricks)              Data Consumer (any client)
┌─────────────────────────┐         ┌──────────────────────────┐
│  Delta Table (source)   │         │  Python / Spark / Power  │
│         │               │         │  BI / Tableau / pandas   │
│         ▼               │ HTTPS   │         ▲                │
│  Share + Recipient ─────┼────────▶│  Delta Sharing Client    │
│  (access tokens)        │ (REST)  │  (reads Parquet directly)│
└─────────────────────────┘         └──────────────────────────┘
```

### Provider Side (Databricks)
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sharing import *

w = WorkspaceClient()

# Create a share
w.shares.create(name="partner-revenue-share")

# Add tables to the share with partition filtering
w.shares.update(
    name="partner-revenue-share",
    updates=[
        SharedDataObjectUpdate(
            action=SharedDataObjectUpdateAction.ADD,
            data_object=SharedDataObject(
                name="gold.daily_revenue_by_region",
                data_object_type="TABLE",
                partitions=[
                    Partition(values=[
                        PartitionValue(name="region", op=PartitionValueOp.EQUAL, value="EMEA")
                    ])
                ]
            )
        )
    ]
)

# Create a recipient (generates a sharing token)
recipient = w.recipients.create(
    name="partner-acme-corp",
    authentication_type=AuthenticationType.TOKEN
)
# Send the activation link to the partner securely
print(recipient.tokens[0].activation_url)
```

### Consumer Side (No Databricks Required)
```python
import delta_sharing

# Load profile (contains server URL + token)
client = delta_sharing.SharingClient("partner_profile.share")

# List available shares
shares = client.list_shares()
tables = client.list_all_tables()

# Read shared table as Pandas DataFrame
df = delta_sharing.load_as_pandas(
    "partner_profile.share#partner-revenue-share.gold.daily_revenue_by_region"
)

# Read as Spark DataFrame
spark_df = delta_sharing.load_as_spark(
    "partner_profile.share#partner-revenue-share.gold.daily_revenue_by_region"
)
```

**Key properties**:
| Property | Detail |
|---|---|
| No data copy | Consumer reads directly via REST protocol |
| No Databricks needed | Consumer uses open-source client |
| Real-time freshness | Consumer always sees latest committed version |
| Partition filtering | Provider controls which partitions are visible |
| Access logging | All reads logged on provider side |
| Revocable | Provider can revoke recipient access instantly |
| Cross-cloud | Share between AWS, Azure, GCP |

**Business value**: Data marketplace, inter-company collaboration, and regulatory reporting without physically transferring data.

---

## 10. Architecture Summary

### What Lakehouse Replaces / Unifies

```
Before Lakehouse                    After Lakehouse (Databricks)
════════════════════                ══════════════════════════════

S3 / ADLS (raw dump)           →    Delta Lake (versioned, ACID)
Amazon Redshift / Synapse      →    Databricks SQL + Photon Engine
AWS Glue / ADF (batch ETL)     →    Delta Live Tables + Auto Loader
Apache Flink (streaming)       →    Structured Streaming on Delta
SageMaker / Azure ML           →    Mosaic AI (train + serve + monitor)
MLflow (self-hosted)           →    Managed MLflow (in Databricks)
Feast / Tecton (feature store) →    Databricks Feature Store
Collibra / Alation (catalog)   →    Unity Catalog (governance)
Pinecone / Weaviate (vector)   →    Databricks Vector Search
OpenAI API (LLMs)              →    Foundation Model APIs (DBRX, Llama)
Immuta / Privacera (masking)   →    Unity Catalog row/column filters
MuleSoft / Fivetran (ingest)   →    Auto Loader + Kafka connector
```

### End-to-End Data Flow

```
External Sources
(Databases, SaaS APIs, Kafka, IoT, Files)
          │
          │  Auto Loader / Kafka / JDBC / REST
          ▼
┌─────────────────────────────────────────────────────┐
│                      BRONZE                          │
│   Raw data, append-only, full fidelity               │
│   Delta Lake on ADLS / S3 / GCS                     │
│   Schema: _rescued_data, ingestion_ts, source_system │
└─────────────────────────────────────────────────────┘
          │
          │  DLT / Spark / Structured Streaming
          │  expect_or_drop, dedup, type cast
          ▼
┌─────────────────────────────────────────────────────┐
│                      SILVER                          │
│   Cleaned, validated, deduplicated                   │
│   Unity Catalog governed, lineage tracked            │
│   Row/column security applied                        │
└─────────────────────────────────────────────────────┘
          │
          │  DLT / Spark SQL / Materialized Views
          ▼
┌─────────────────────────────────────────────────────┐
│                       GOLD                           │
│   Business KPIs, Feature tables, ML-ready            │
│   Optimized for query (Z-ORDER / Liquid Clustering)  │
└─────────────────────────────────────────────────────┘
          │
    ┌─────┼──────────────────────────────┐
    │     │                              │
    ▼     ▼                              ▼
BI / SQL Analytics            Machine Learning / AI
├─ Databricks SQL             ├─ Feature Store
├─ Power BI / Tableau         ├─ MLflow Tracking + Registry
├─ Looker / Qlik              ├─ AutoML
├─ Dashboards + Alerts        ├─ Model Serving (REST)
└─ Delta Sharing              ├─ Vector Search + RAG
                              ├─ Foundation Model APIs
                              └─ LLM Fine-Tuning
```

### Unified Security Model

```
Unity Catalog (one policy for all assets)
├── WHO   →  Users / Groups / Service Principals (SCIM synced from IdP)
├── WHAT  →  Catalogs / Schemas / Tables / Views / Volumes / Models / Functions / Shares
├── HOW   →  SELECT / MODIFY / CREATE / MANAGE / EXECUTE / READ_FILES / WRITE_FILES
├── WHERE →  Catalog-level managed storage location (region pinning)
├── MASK  →  Column masks (PII), Row filters (multi-tenancy)
├── TRACE →  Automatic column-level lineage across all transforms
└── AUDIT →  system.access.audit → CloudTrail / Azure Monitor / Splunk / SIEM
```
