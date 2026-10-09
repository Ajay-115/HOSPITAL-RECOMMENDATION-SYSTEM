# Hadoop / HDFS Integration Guide
**Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System**

This directory prepares the project for Big Data HDFS storage and retrieval (Phases 6-7).

---

## 1. Expected Data Flow
```
Raw CSV / Streaming Logs
        │
        ▼
   Hadoop HDFS (/data/healthcare/)
        │
        ▼
   PySpark Batch & Aggregation Jobs
        │
        ▼
   Processed Resource Metrics (/output/analytics/)
        │
        ▼
   Flask REST Backend
        │
        ▼
   Frontend Dashboard
```

---

## 2. Hadoop Setup Instructions (Academic / Local Demonstration)

### Step 1: Start Hadoop Daemons
Ensure Hadoop environment variables (`HADOOP_HOME`, `JAVA_HOME`) are configured.
```bash
start-dfs.cmd
start-yarn.cmd
```

### Step 2: Create HDFS Project Directories
```bash
hdfs dfs -mkdir -p /healthcare/datasets
hdfs dfs -mkdir -p /healthcare/processed
```

### Step 3: Upload Datasets to HDFS
```bash
hdfs dfs -put ../backend/data/hospitals.csv /healthcare/datasets/
hdfs dfs -put ../backend/data/surgeries.csv /healthcare/datasets/
hdfs dfs -put ../backend/data/blood_banks.csv /healthcare/datasets/
```

### Step 4: Verify Upload
```bash
hdfs dfs -ls /healthcare/datasets/
```

### Step 5: Run PySpark Jobs on HDFS Data
```bash
spark-submit ../spark/hospital_analytics.py
```

---
*Note: In Phase 1, the frontend runs with local synthetic data and does not mandate active HDFS clusters.*
