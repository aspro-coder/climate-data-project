# Big Data Project for Historical and Real-Time Weather Analysis

A Big Data pipeline for historical batch analytics and real-time weather stream processing using:

- Apache Spark
- Hadoop HDFS
- Kafka
- Elasticsearch
- Kibana
- PostgreSQL
- Metabase
- Docker Compose

The project combines a **Batch Processing Layer** for long-term historical weather analysis and a **Streaming Layer** for real-time weather monitoring and alert detection.

---

# Table of Contents

1. [Project Architecture](#1-project-architecture)
2. [Prepare Data](#2-prepare-data)
3. [Automation & Setup](#3-automation--setup)
4. [Infrastructure Verification](#4-infrastructure-verification)
5. [Batch Processing Layer](#5-batch-processing-layer)
6. [Metabase Integration](#6-business-intelligence-dashboard-integration-metabase)
7. [Streaming Layer](#7-streaming-layer-streaming-pipeline)
8. [Kibana Monitoring](#8-real-time-analytics--alert-tracking-kibana)
9. [Analytics Catalog](#9-comprehensive-analytics-catalog)

---

# 1. Project Architecture

All services are containerized and managed using **Docker Compose**.

## Architecture Diagram

![Project Architecture](architecture_diagram.png)

---

# 2. Prepare Data

Before running the pipeline, the required datasets must be downloaded and placed into the correct project directories.

## A) Historical Batch Dataset

Due to GitHub file size limitations, the primary historical dataset (~2–3 GB) is excluded from version control.

### Dataset Source

Download the dataset from Kaggle:

https://www.kaggle.com/datasets/bwandowando/forbes-top-100-cities-weather-data1990-1999

### Required File Name

```bash
hourly_data_combined_1990_to_1999.csv
```

### Placement

Place the dataset inside:

```bash
work/
```

> The `work/` directory is protected using `.gitignore` to prevent accidental uploads of large files.

---

## B) Real-Time Streaming Dataset

The streaming layer uses a prepared OpenWeatherMap JSON dataset that simulates live sensor telemetry.

### Location

```bash
streaming/api_data/api_data.json
```

### Notes

- Contains 255 JSON weather records
- Automatically consumed by `weather_producer.py`
- No manual preparation required

---

# 3. Automation & Setup

The project infrastructure and processing pipelines are automated through a collection of shell scripts located inside the `work/` directory.

## Run the Full Pipeline

```bash
./work/master_pipeline.sh
```

This command:

- Starts the infrastructure
- Loads historical datasets into HDFS
- Executes PySpark transformations
- Runs analytical queries
- Loads results into PostgreSQL
- Starts the real-time streaming console

---

## Script Overview

### `master_pipeline.sh`

Main orchestration script responsible for coordinating both the batch and streaming layers.

Features:

- Automatic pipeline termination on batch errors using `set -e`
- Automatic transition from batch to streaming mode
- Interactive streaming console support

---

### `run_batch_pipeline.sh`

Automates the complete historical processing workflow.

Pipeline stages:

1. Infrastructure initialization and HDFS preparation
2. PySpark transformation and cleaning jobs
3. Execution of analytical Spark queries
4. Writing processed results into PostgreSQL

---

### `run_realtime_pipeline.sh`

Interactive menu for running real-time streaming jobs.

Features:

- Launches 5 independent streaming jobs
- Automatically clears old checkpoints from HDFS
- Resets Elasticsearch indices before execution

---

## Linux Encoding Fix (Optional)

If scripts were edited on Windows, carriage return characters (`\r`) may cause execution issues on Linux.

Fix the scripts using:

```bash
sudo apt-get update && sudo apt-get install -y dos2unix

dos2unix work/*.sh
chmod +x work/*.sh
```

---

## Stop the Cluster

```bash
docker-compose down
```

---

# 4. Infrastructure Verification

After the containers are running, the following web interfaces can be used to monitor the infrastructure:

| Service | URL |
|---|---|
| Hadoop HDFS | http://localhost:9870 |
| Apache Spark | http://localhost:8080 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |
| Metabase | http://localhost:3000 |

---

# 5. Batch Processing Layer

The batch layer analyzes historical weather data collected between 1990 and 1999.

## Processing Workflow

### Data Transformation

Raw CSV data is:

- Loaded from HDFS
- Cleaned and validated
- Converted into optimized Parquet format
- Stored inside the transformed data zone

Target HDFS path:

```bash
hdfs://namenode:9000/data/transformed_zone/transformed_weather_data
```

---

### Analytical Queries

The system executes 11 PySpark analytical jobs over the transformed Parquet datasets.

Analytical results are stored in PostgreSQL using JDBC.

### PostgreSQL Configuration

```text
JDBC URL: jdbc:postgresql://postgresql:5432/postgres
Username: postgres
Password: postgres
```

Example analytical table:

```text
diurnal_temp_variation
```

---

# 6. Business Intelligence Dashboard Integration (Metabase)

Once analytical results are loaded into PostgreSQL, Metabase can be connected for interactive dashboard creation and visualization.

## Metabase Setup

Open:

```text
http://localhost:3000
```

Then configure the PostgreSQL connection using:

| Parameter | Value |
|---|---|
| Database Type | PostgreSQL |
| Host | postgresql |
| Port | 5432 |
| Database Name | postgres |
| Username | postgres |
| Password | postgres |

After connecting:

- Run schema synchronization
- Explore analytical tables
- Create dashboards, charts, and visual reports

---

## Example SQL Query

```sql
SELECT city_name,
       avg_temp_variation,
       max_single_day_variation
FROM diurnal_temp_variation
WHERE rank <= 10;
```

---

# 7. Streaming Layer (Streaming Pipeline)

The streaming layer simulates a live weather monitoring system using Kafka and Structured Streaming.

## Architecture Components

### Producer

The `weather-producer` service continuously reads weather events from the API dataset and publishes them to Kafka topics.

---

### Consumer Subsystem

Streaming consumers perform the following tasks:

- Read Kafka messages
- Clean incoming events
- Process streaming windows
- Send results into Elasticsearch

---

### Automatic Cleanup

Before starting a new streaming job, the system automatically:

- Removes old HDFS checkpoints
- Clears Elasticsearch indices
- Resets previous query states

---

# 8. Real-Time Analytics & Alert Tracking (Kibana)

Kibana is used for real-time monitoring and visualization of streaming analytics results.

## Setup Instructions

Open:

```text
http://localhost:5601
```

Then:

1. Navigate to **Analytics → Discover**
2. Create a new Data View
3. Use index patterns such as:

```text
realtime_*
```

4. Select the timestamp field
5. Save the Data View

Streaming dashboards and visualizations will then be available in Kibana.

---

# 9. Analytics Overview

## A) Historical Batch Analytics (1990–1999)

### Query 1

Which cities had the highest average temperature variations between daytime and nighttime during the period 1990–1999?

### Query 2

How did average temperature, humidity, and precipitation change monthly across cities during the period 1990–1999?

### Query 3

Which cities recorded the highest number of snow days during each winter season (December, January, February) within each year from 1990 to 1999?

### Query 4

Which cities experienced the highest number of extreme temperature days (below -10°C or above 40°C) during each year in the period 1990–1999?

### Query 5

How did average atmospheric pressure and precipitation vary between summer and winter months during the period 1990–1999?

### Query 6

How did average wind speeds change across cities during different seasons, including seasonal variations within each year from 1990 to 1999?

### Query 7

Which cities had the lowest number of sunshine hours during winter seasons (December, January, February) for each year in the period 1990–1999?

### Bonus Query

How much did winter sunshine duration deviate from the long-term historical average for each city during the period 1990–1999?

### Query 8

Which cities recorded the highest precipitation levels during days with temperatures above 30°C, and what percentage of those hot days included rainfall, both yearly and overall during the period 1990–1999?

### Query 9

Which 10 cities had the largest humidity variations between daytime and nighttime during the period 1990–1999, and how were they ranked yearly and overall across the entire observed period?

### Query 10

Is there a correlation between precipitation and temperature across cities during the period 1990–1999? Do cities with lower precipitation generally experience higher temperatures compared to cities with higher precipitation levels?

---

## B) Real-Time Streaming Analytics

### Job 1 — Abu Dhabi Temperature Monitoring

What is the average temperature in Abu Dhabi during the last 60 minutes compared to the historical monthly average temperatures for the same month across each year from 1990 to 1999, and what are the absolute and percentage deviations?

### Job 2 — Bangkok Flash Flood Risk Detection

Has Bangkok experienced, during the last 90 minutes:

* a sudden increase in cloud coverage (more than 30% above the long-term monthly average for the same month during 1990–1999), and
* a simultaneous atmospheric pressure drop (more than 3 hPa within the last 90 minutes and at least 2 hPa below the long-term monthly average),

which could indicate a risk of intense rainfall and urban flooding?

### Job 3 — Dubai Heatstroke Risk Monitoring

Has Dubai experienced during the last 30 minutes:

* a sudden humidity increase greater than 20%, or
* humidity above 85% while temperatures exceed 35°C,

which may indicate an increased risk of heatstroke conditions?

### Job 4 — Chicago Wind Anomaly Detection

Has Chicago experienced during the last 30 minutes:

* a sudden increase in wind speed greater than 50%,
* wind gust increases above 30%, or
* wind direction changes greater than 45°?

### Job 5 — Beijing Atmospheric Pressure Monitoring

What is the atmospheric pressure in Beijing during the last 60 minutes compared to historical monthly average pressure values for June across each year from 1990 to 1999, and what are the absolute and percentage deviations?

