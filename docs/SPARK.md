# Apache Spark / PySpark Big Data Processing Layer — MediPulse

## 1. Overview & Objectives

In **Phase 6**, MediPulse integrates an enterprise-grade **Apache Spark / PySpark** distributed batch-processing pipeline. 

While the operational Flask web service and SQLite database manage low-latency online transactional processing (OLTP), real-time recommendation scoring, and administrative CRUD operations, the PySpark analytics engine provides high-throughput online analytical processing (OLAP) across four primary healthcare domain datasets:
1. **Hospitals** (`hospitals.csv` / `hospitals` table)
2. **Medical Equipment & Resources** (`resources.csv` / `hospital_resources` table)
3. **Surgeries & Operating Theatres** (`surgeries.csv` / `surgeries` table)
4. **Blood Bank Reserves** (`blood_banks.csv` / `blood_banks` table)

The pipeline is completely isolated from the live Flask service, ensuring that analytical batch queries do not introduce contention, resource exhaustion, or lock escalation into operational databases.

---

## 2. Big Data Architecture

```
+-----------------------------------------------------------------------------------+
|                            MediPulse Big Data Architecture                        |
+-----------------------------------------------------------------------------------+

     [ Operational Data Sources ]
           |              |
           v              v
     backend/data/*.csv  SQLite DB (app.db)
           |
           | Ingestion with Explicit StructType Schemas
           v
+-----------------------------------------------------------------------------------+
|                               APACHE SPARK ENGINE                                 |
|                                                                                   |
|  1. Data Ingestion (spark/data_ingestion.py)                                     |
|     - Strict StructType schemas (Hospital, Resource, Surgery, Blood Bank)         |
|     - Fail-safe handling for malformed rows and type safety                       |
|                                                                                   |
|  2. Distributed Cleaning & Validation (spark/data_cleaning.py)                    |
|     - ID deduplication (dropDuplicates on primary keys)                           |
|     - String trimming & case standardization (Title Case, Upper Case)             |
|     - Numeric boundary clamping:                                                  |
|         * Total Beds >= 1, Available Beds clamped to [0, Total Beds]              |
|         * ICU Beds >= 0, Available ICU clamped to [0, Total ICU]                  |
|         * Rating clamped to [1.0, 5.0]                                            |
|         * Costs clamped to >= 0                                                   |
|         * Wait times clamped to >= 5 mins                                         |
|                                                                                   |
|  3. Distributed Feature Transformations (spark/transformations.py)                |
|     - Bed Occupancy Rate: ((total_beds - available_beds) / total_beds) * 100      |
|     - ICU Occupancy Rate: ((total_icu - available_icu) / total_icu) * 100         |
|     - Resource Utilization: (in_use_units / total_units) * 100                    |
|     - Cost Tier Classification: Low (< 15k) / Medium (15k-35k) / High (> 35k)   |
|     - Waiting Time Tier: Fast (< 30m) / Moderate (30-60m) / High (> 60m)          |
|     - Relational multi-table joins on hospital_id                                 |
|                                                                                   |
|  4. Aggregation & Analytical Engines                                              |
|     - Hospital Analytics (spark/hospital_analytics.py)                            |
|     - Resource Analytics (spark/resource_analytics.py)                            |
|     - Recommendation & Cross-Domain Analytics (spark/recommendation_analytics.py) |
+-----------------------------------------------------------------------------------+
           |
           | Output Layer (Snappy-compressed Parquet + Human-Readable CSV)
           v
+-----------------------------------------------------------------------------------+
|                             spark/output/ (Data Lake)                             |
|                                                                                   |
|  - hospital_summary/       (Enriched Hospital Parquet + CSV)                      |
|  - district_analytics/     (District capacity, occupancy & emergency load)        |
|  - specialty_analytics/    (Specialty coverage, waiting times & procedures)       |
|  - resource_analytics/     (Critical equipment utilization & availability)        |
|  - surgery_analytics/      (Surgery cost, duration & risk tier statistics)        |
|  - blood_analytics/        (Blood stock metrics & critical shortage alerts)       |
+-----------------------------------------------------------------------------------+
```

---

## 3. Data Flow

```
CSV Datasets
    |
    v
[ spark/data_ingestion.py ]
    | Reads CSV with StructType schema (explicit types, nullable flags)
    v
Raw Spark DataFrames
    |
    v
[ spark/data_cleaning.py ]
    | Deduplication, whitespace trimming, numeric clamping, default imputation
    v
Cleaned DataFrames
    |
    v
[ spark/transformations.py ]
    | Feature engineering: occupancy %, utilization %, cost tiers, wait tiers, joins
    v
Enriched Analytical DataFrames
    |
    v
[ spark/hospital_analytics.py ]
[ spark/resource_analytics.py ]
[ spark/recommendation_analytics.py ]
    | GroupBy, aggregations, statistical summaries, critical shortage flags
    v
Analytical Summary DataFrames
    |
    v
[ spark/pipeline.py ] -> Disk Writer
    | Partitioned Parquet (Snappy compressed) + Coalesced Single-CSV Inspection Exports
    v
spark/output/
```

---

## 4. SparkSession Configuration

The SparkSession is initialized via `spark/spark_session.py` with custom environment detection tailored for both Windows and UNIX systems:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MediPulse-HospitalAnalytics") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()
```

### Windows Portability & Native Winutils Integration
On Windows workstations, PySpark relies on Hadoop native libraries (`winutils.exe` and `hadoop.dll`) for Windows filesystem access and FileOutputCommitter operations. `spark_session.py` automatically:
1. Detects or configures `HADOOP_HOME` pointing to `hospital_recommendation_system/hadoop`.
2. Prepends `hadoop/bin` to `os.environ["PATH"]`.
3. Auto-discovers local Java runtimes (e.g. OpenJDK 17) and sets `JAVA_HOME`.

---

## 5. Cleaning Rules Applied

| Domain | Attribute | Cleaning Logic |
| :--- | :--- | :--- |
| **All** | Primary Keys (`hospital_id`, `surgery_id`, `bank_id`, `resource_id`) | `.dropDuplicates(subset=[pk])` |
| **All** | Strings (`name`, `city`, `district`, `specialty`, etc.) | `.withColumn(col, trim(initcap(col)))` |
| **Hospitals** | `total_beds` | Clamped to `when(col < 1, 1).otherwise(col)` |
| **Hospitals** | `available_beds` | Clamped to `when(col < 0, 0).when(col > total_beds, total_beds)` |
| **Hospitals** | `total_icu_beds` | Clamped to `when(col < 0, 0).otherwise(col)` |
| **Hospitals** | `available_icu_beds` | Clamped to `when(col < 0, 0).when(col > total_icu_beds, total_icu_beds)` |
| **Hospitals** | `rating` | Clamped strictly to `[1.0, 5.0]` |
| **Hospitals** | `average_cost` | Clamped to `>= 0.0` |
| **Hospitals** | `estimated_wait_time_mins`| Clamped to `>= 5` mins |
| **Resources** | `total_units` | Clamped to `>= 0` |
| **Resources** | `in_use_units` | Clamped to `[0, total_units]` |
| **Surgeries** | `estimated_cost` | Clamped to `>= 0.0` |
| **Blood Banks** | `units_available` | Clamped to `>= 0` |

---

## 6. Feature Transformations and Formulas

### A. Bed Occupancy Percentage
Evaluates inpatient bed pressure:
$$\text{bed\_occupancy\_percentage} = \left( \frac{\text{total\_beds} - \text{available\_beds}}{\text{total\_beds}} \right) \times 100$$
*(Rounded to 2 decimal places, safely guarded against division by zero).*

### B. ICU Occupancy Percentage
Evaluates critical care bed saturation:
$$\text{icu\_occupancy\_percentage} = \begin{cases} \left( \frac{\text{total\_icu\_beds} - \text{available\_icu\_beds}}{\text{total\_icu\_beds}} \right) \times 100 & \text{if } \text{total\_icu\_beds} > 0 \\ 0.0 & \text{otherwise} \end{cases}$$

### C. Resource Utilization Percentage
Tracks equipment allocation (e.g. Ventilators, Dialysis machines):
$$\text{resource\_utilization\_pct} = \begin{cases} \left( \frac{\text{in\_use\_units}}{\text{total\_units}} \right) \times 100 & \text{if } \text{total\_units} > 0 \\ 0.0 & \text{otherwise} \end{cases}$$

### D. Cost Tiers
- **Low**: $\text{average\_cost} < 15,000$
- **Medium**: $15,000 \le \text{average\_cost} \le 35,000$
- **High**: $\text{average\_cost} > 35,000$

### E. Waiting Time Tiers
- **Fast**: $\text{wait\_time} < 30\text{ min}$
- **Moderate**: $30\text{ min} \le \text{wait\_time} \le 60\text{ min}$
- **High**: $\text{wait\_time} > 60\text{ min}$

---

## 7. Aggregation Queries & Statistical Analytics

1. **District-Level Capacity & Demand (`spark/hospital_analytics.py`):**
   - Aggregates by `district`: total hospitals, total beds, total available beds, total ICU beds, total available ICU beds, average rating, average wait time, and average treatment cost.
   - Calculates district-wide macro bed and ICU occupancy rates.

2. **Specialty Distribution & Quality Metrics:**
   - Aggregates by `specialties` / `medical_specialty`: hospital count, average rating, min/max/avg wait time, and min/max/avg procedure cost.

3. **Resource Saturation & Equipment Availability (`spark/resource_analytics.py`):**
   - Aggregates by `resource_type`: total units across state, units actively in use, available units, and average utilization %.
   - Identifies high-utilization equipment bottlenecks where utilization exceeds 85%.

4. **Surgery Economics & Duration Profiling (`spark/recommendation_analytics.py`):**
   - Aggregates by `specialty` & `risk_level`: procedure count, avg duration (minutes), avg cost, min cost, max cost, and avg success rate.

5. **Blood Bank Reserve Surveillance:**
   - Aggregates by `blood_group`: total units available across all certified blood banks.
   - Identifies blood shortage alerts where total statewide inventory drops below threshold.

---

## 8. Execution Instructions

### A. Run Full End-to-End Pipeline

**1. Local Mode (Default):**
```powershell
$env:MEDIPULSE_DATA_SOURCE="local"
.\venv\Scripts\python.exe -m spark.pipeline
```
*Ingests from `backend/data/*.csv` on the local filesystem.*

**2. HDFS Mode (Hadoop Cluster in WSL2):**
```powershell
$env:MEDIPULSE_DATA_SOURCE="hdfs"
.\venv\Scripts\python.exe -m spark.pipeline
```
*Ingests directly from `hdfs://localhost:9000/medipulse/raw/production/*.csv`.*


### B. Run Individual Domain Batch Jobs
```powershell
python -m spark.jobs.process_hospitals
python -m spark.jobs.process_resources
python -m spark.jobs.process_surgeries
python -m spark.jobs.process_blood
```

### C. Run the PySpark Unit Test Suite
```powershell
python -m unittest discover -s spark/tests -v
```

### D. Run the Operational Application Regression Test Suite
```powershell
python -m unittest discover -s tests -v
```

---

## 9. Spark Output Directory Structure

The pipeline generates analytical datasets stored in `spark/output/`:

```
spark/output/
├── district_analytics/
│   ├── _SUCCESS
│   ├── part-*.parquet
│   └── district_analytics_summary.csv
├── specialty_analytics/
│   ├── _SUCCESS
│   ├── part-*.parquet
│   └── specialty_analytics_summary.csv
├── hospital_summary/
│   ├── _SUCCESS
│   ├── part-*.parquet
│   └── hospital_summary_preview.csv
├── resource_analytics/
│   ├── _SUCCESS
│   ├── part-*.parquet
│   └── resource_analytics_summary.csv
├── surgery_analytics/
│   ├── _SUCCESS
│   ├── part-*.parquet
│   └── surgery_analytics_summary.csv
└── blood_analytics/
    ├── _SUCCESS
    ├── part-*.parquet
    └── blood_analytics_summary.csv
```

Each directory contains:
- **Snappy Parquet files**: Columnar, distributed format optimized for Spark, Presto, Trino, and Hive.
- **CSV Summary files**: Coalesced inspection files suitable for quick verification and lightweight dashboarding.

---

## 10. Architectural Evolution Across Phases

| Phase | Core Focus | Implementation Details |
| :--- | :--- | :--- |
| **Phase 6** | PySpark Local Batch Analytics | Core schemas, distributed cleaning, occupancy formulas, and unit tests. |
| **Phase 7** | Hadoop Distributed File System (HDFS) | Ingestion from `hdfs://localhost:9000/medipulse/raw/production/` and Parquet sink to `hdfs://localhost:9000/medipulse/processed/` and `/medipulse/analytics/`. |
| **Phase 8** | Spark Structured Streaming Simulation | File-based continuous telemetry ingestion, watermarking, 1-minute tumbling windows, HDFS checkpointing, and real-time Parquet metrics sink. |

---

## 11. Phase 8 — Spark Structured Streaming Simulation

### A. Why Structured Streaming?
In high-acuity healthcare environments, static batch analytics alone cannot capture rapid fluctuations in emergency room volume, sudden ICU bed saturation, or acute ventilator demand. 

**Spark Structured Streaming** provides an incremental, fault-tolerant stream-processing engine built upon the Spark SQL Catalyst optimizer. It processes streams as an unbounded table, enabling exactly-once processing guarantees and dynamic windowed aggregations without needing separate stream-processing codebases.

### B. File-Based Streaming Simulation
In production enterprise architectures, event telemetry is often streamed via message brokers such as Apache Kafka or AWS Kinesis. For academic demonstration and Windows/WSL2 deployment without heavy broker dependencies:
- **File-Based Structured Streaming** is employed (`spark.readStream.schema(...).json(input_path)`).
- Synthetic hospital resource telemetry micro-batches are continuously written as JSON files into `hdfs://localhost:9000/medipulse/raw/streaming/`.
- Spark Structured Streaming automatically detects new arriving files, ingests the events into the streaming DataFrame, updates state stores, and writes output partitions incrementally.

### C. Event Schema
Defined in [`spark/streaming/streaming_schema.py`](file:///c:/Users/ajays/Downloads/HOSPITAL%20RECOMMENDATION/hospital_recommendation_system/spark/streaming/streaming_schema.py):

| Field | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `event_id` | `StringType` | `True` | Unique event UUID |
| `timestamp` | `TimestampType` | `True` | Event generation timestamp |
| `hospital_id` | `StringType` | `True` | Hospital identifier (`H101` to `H124`) |
| `resource_type` | `StringType` | `True` | Resource category (`ICU`, `General Bed`, `Ventilator`, `Operation Theatre`, `Blood Unit`) |
| `total_capacity` | `IntegerType` | `True` | Configured resource capacity |
| `occupied` | `IntegerType` | `True` | Currently occupied or in-use units |
| `available` | `IntegerType` | `True` | Available units ready for allocation |
| `emergency_load` | `StringType` | `True` | Current emergency saturation (`Low`, `Medium`, `High`, `Critical`) |

### D. Data Cleaning & Validation Invariants
The streaming query filters out malformed or out-of-bounds telemetry:
- `hospital_id.isNotNull()` and `resource_type.isNotNull()` and `timestamp.isNotNull()`
- `occupied >= 0` and `available >= 0` and `total_capacity >= 0`
- `occupied <= total_capacity` and `available <= total_capacity`

### E. Real-Time Feature Engineering: Utilization Percentage
$$\text{utilization\_percentage} = \begin{cases} \left( \frac{\text{occupied}}{\text{total\_capacity}} \right) \times 100 & \text{if } \text{total\_capacity} > 0 \\ 0.0 & \text{otherwise} \end{cases}$$

### F. Watermarking & 1-Minute Tumbling Windows
- **Watermark**: `.withWatermark("timestamp", "1 minute")` bounds state retention, allowing Spark to drop obsolete state for windows that have already finalized.
- **Window Aggregation**: Grouped by `window(col("timestamp"), "1 minute")`, `col("hospital_id")`, `col("resource_type")`.
- **Calculations**:
  - `avg_utilization`: Average resource utilization percentage in window.
  - `avg_occupied`: Average occupied units.
  - `avg_available`: Average available units.
  - `max_emergency_load`: Peak emergency load observed.
  - `event_count`: Number of telemetry events in window.

### G. HDFS Checkpoint & Parquet Output
- **Output Mode**: `append` (compatible with window aggregations and watermarks).
- **HDFS Output**: `hdfs://localhost:9000/medipulse/analytics/realtime_resource_metrics/`
- **HDFS Checkpoint**: `hdfs://localhost:9000/medipulse/checkpoints/resource_streaming/`
- **Query Name**: `medipulse-resource-stream`

### H. Synthetic Data & Privacy Compliance
All telemetry events are generated synthetically using realistic hospital capacity and occupancy boundaries. No personal identifying information (PII), patient names, phone numbers, addresses, or medical record numbers (MRNs) are ever generated or stored.

---

## 12. Verification & Execution Commands for Phase 8

### A. Run Streaming Unit Tests
```powershell
.\venv\Scripts\python.exe -m unittest discover -s spark/streaming/tests -v
```

### B. Run End-to-End Structured Streaming Simulation (HDFS)
```powershell
$env:MEDIPULSE_DATA_SOURCE="hdfs"
.\venv\Scripts\python.exe -m spark.streaming.streaming_pipeline
```

### C. Verify HDFS Streaming Output and Checkpoints via WSL2 Ubuntu
```bash
# List real-time Parquet metrics generated in HDFS
hdfs dfs -ls -R /medipulse/analytics/realtime_resource_metrics

# Inspect checkpoint commits and offset tracking
hdfs dfs -ls -R /medipulse/checkpoints/resource_streaming
```

### D. Read Real-Time Metrics back with PySpark
```powershell
.\venv\Scripts\python.exe -c "
from spark.spark_session import get_spark_session
spark = get_spark_session('verify_stream')
df = spark.read.parquet('hdfs://localhost:9000/medipulse/analytics/realtime_resource_metrics')
print('=== Real-Time Metrics Count:', df.count(), '===')
df.show(10, truncate=False)
spark.stop()
"
```

