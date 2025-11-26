# pyspark-kafka-airflow

A simple end-to-end data pipeline using **Kafka**, **PySpark Structured Streaming**, and **Airflow** – all running in Docker.

Data flow:

1. A Python **producer** sends JSON events to **Kafka**.
2. A **Spark Structured Streaming** job reads from Kafka and writes **partitioned Parquet** to a shared volume.
3. An **Airflow DAG** periodically reads the Parquet data with **pandas + pyarrow** and logs simple aggregations.

This project is designed to be **minimal but realistic** – good for learning, demos, and interview prep.

---

## 1. Architecture Overview

**Services (via `docker-compose.yml`):**

- `zookeeper` – Zookeeper for Kafka.
- `kafka` – Kafka broker (`kafka:9092` inside Docker, `localhost:9094` for host).
- `kafka-init` – One-shot container to create the Kafka topic.
- `spark-master` – Spark master (`spark://spark-master:7077`).
- `spark-worker` – Spark worker that connects to the master.
- `spark-streaming` – Runs the PySpark Structured Streaming job (`spark/app.py`).
- `producer` – Python process that sends random JSON events to Kafka.
- `airflow` – Airflow webserver + scheduler + DAGs.

**Storage layout:**

- On host: `./data/output`
- In Spark containers: `/opt/spark-output`
- In Airflow: `/opt/airflow/data`

Spark writes Parquet under:

```text
./data/output/parquet/run_<RUN_ID>/id=<id>/part-*.parquet
````

Airflow reads from:

```text
/opt/airflow/data/parquet   # which is ./data/output/parquet on the host
```

---

## 2. Project Structure

```text
pyspark-kafka-airflow/
├── docker-compose.yml
├── .env
├── data/
│   └── output/
│       └── parquet/                # Spark writes here
├── cache/
│   └── ivy/                        # Spark Ivy cache (created at runtime)
├── spark/
│   └── app.py                      # Spark Structured Streaming job
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py                 # Kafka producer sending JSON events
└── airflow/
    ├── Dockerfile
    ├── requirements.txt
    └── dags/
        └── spark_parquet_validation_dag.py
```

---

## 3. Pre-requisites

* Docker & Docker Compose (Compose V2)
* (Optional) Python for editing/running scripts locally
* Tested on Apple Silicon (`arm64`) with:

  * `confluentinc/cp-kafka:7.9.3.arm64`
  * `spark:3.5.1-python3`
  * `apache/airflow:2.9.3-python3.11`

---

## 4. Configuration

### `.env`

```env
KAFKA_TOPIC=events
MSGS_PER_SEC=5
```

* `KAFKA_TOPIC` – Kafka topic used by producer, Spark, and `kafka-init`.
* `MSGS_PER_SEC` – How many messages per second the producer sends.

### Key ports

* **Kafka (external)**: `localhost:9094`
* **Spark Master UI**: `http://localhost:8080`
* **Spark Worker UI**: `http://localhost:8081`
* **Airflow Web UI**: `http://localhost:8082`

---

## 5. Running the Stack

From the project root:

### 5.1. Create local folders

```bash
mkdir -p cache/ivy
mkdir -p data/output
```

### 5.2. Build and start

```bash
docker compose up -d --build
```

Check containers:

```bash
docker compose ps
```

You should see `zookeeper`, `kafka`, `kafka-init` (exits after success), `spark-master`, `spark-worker`, `spark-streaming`, `producer`, and `airflow`.

---

## 6. What Each Component Does

### 6.1. Producer → Kafka (`producer/producer.py`)

* Uses `kafka-python` to send events:

```json
{
  "id": <0-4>,
  "ts": "<ISO8601 UTC timestamp>",
  "value": <random double 0-100>,
  "source": "sim-producer"
}
```

* Controlled by env vars:

  * `KAFKA_BOOTSTRAP_SERVERS` (default `kafka:9092`)
  * `KAFKA_TOPIC` (default `events`)
  * `MSGS_PER_SEC` (default `5`)

Check logs:

```bash
docker logs -f producer
```

You should see something like:

```text
Producer starting. Kafka=kafka:9092, topic=events, msgs/sec=5
```

---

### 6.2. Spark Streaming (`spark/app.py`)

* Reads from Kafka topic `events` using Structured Streaming.
* Parses the JSON into schema:

```python
id: Integer
ts: Timestamp
value: Double
source: String
```

* Two outputs:

1. **Console sink**: prints 10-second window averages per `id` to logs.

   ```bash
   docker logs -f spark-streaming
   ```

2. **Parquet sink**: writes raw parsed events to partitioned Parquet:

   * Path inside container: `/opt/spark-output/parquet/run_<RUN_ID>`
   * Path on host: `./data/output/parquet/run_<RUN_ID>`

   Partitioned by `id`, with a Structured Streaming checkpoint under `_chk`.

---

### 6.3. Airflow (`airflow` service)

* Runs:

  * SQLite DB at `/opt/airflow/airflow.db`
  * `SequentialExecutor` (simple single-process executor)
  * Webserver + scheduler in one container

* On startup:

  * Initializes DB
  * Ensures `default_pool` exists
  * Creates admin user (`admin` / `admin`)

Access the UI at:

```text
http://localhost:8082
```

Login:

* **Username**: `admin`
* **Password**: `admin`

---

## 7. Airflow DAG: `spark_parquet_validation_dag`

File: `airflow/dags/spark_parquet_validation_dag.py`

**Schedule**: every 5 minutes (`*/5 * * * *`)

### What it does

1. Looks for Parquet files under `/opt/airflow/data/parquet` (which maps to `./data/output/parquet` on host).
2. If the folder or files don’t exist, logs a helpful message and exits.
3. Uses **pandas + pyarrow** to read the entire Parquet tree (including partitioned subdirs).
4. Logs:

   * `df.head(10)` – first 10 rows
   * Per-`id` `count` and `mean(value)`
   * Total row count

### Viewing DAG & logs

1. In Airflow UI:

   * Turn `spark_parquet_validation_dag` **ON**.
   * Trigger it manually (play button) or wait for schedule.
2. Click the `aggregate_parquet` task → **Logs**.
3. You should see something like:

```text
[aggregate_parquet] Reading Parquet from: /opt/airflow/data/parquet
=== DataFrame head ===
   id                        ts  value       source
0   1  2025-11-26 12:34:56+00:00  45.123  sim-producer
...

=== Count per id ===
   id  count       mean
0   0   250   49.876
1   1   245   52.341
...

[aggregate_parquet] Total rows: 1234
```

---

## 8. Useful Commands

### View running containers

```bash
docker compose ps
```

### Follow logs

```bash
# Kafka broker
docker logs -f kafka

# Spark streaming job
docker logs -f spark-streaming

# Producer
docker logs -f producer

# Airflow (scheduler + webserver)
docker logs -f airflow
```

### Exec into containers

```bash
docker exec -it spark-master bash
docker exec -it airflow bash
docker exec -it kafka bash
```

---