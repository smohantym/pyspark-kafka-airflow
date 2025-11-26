## 0. Big-picture data flow

End-to-end pipeline:

1. **Producer** (Python)
   → Sends JSON events to **Kafka** topic `events`.

2. **Spark Streaming** (`spark-streaming` container)
   → Reads from Kafka using Structured Streaming
   → Writes **raw events** as partitioned Parquet to a shared folder.

3. **Airflow DAG**
   → Every 5 minutes, uses **pandas + pyarrow** to read the Parquet folder
   → Prints a preview and simple aggregations to Airflow logs.

Shared storage is the `./data/output` folder on your host, mounted into:

* Spark containers as: `/opt/spark-output`
* Airflow as: `/opt/airflow/data`

---

## 1. `docker-compose.yml` – the platform wiring

### Zookeeper

```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.9.3.arm64
  ...
```

* Runs Zookeeper on port **2181**.
* Healthcheck uses `zookeeper-shell` to ensure the service is ready.

### Kafka broker

```yaml
kafka:
  image: confluentinc/cp-kafka:7.9.3.arm64
  depends_on:
    zookeeper:
      condition: service_healthy
  ports:
    - "9092:9092"
    - "9094:9094"
  environment:
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_LISTENERS: PLAINTEXT://:9092,PLAINTEXT_HOST://:9094
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9094
    ...
  volumes:
    - kafka-data:/var/lib/kafka/data
```

* Internal broker address: `kafka:9092` (for other containers).
* External address: `localhost:9094` (for tools on your Mac).
* Data persisted in named volume `kafka-data`, so Kafka logs survive restarts.
* Healthcheck lists topics to ensure broker is actually responsive.

### `kafka-init` – one-shot topic creator

```yaml
kafka-init:
  image: confluentinc/cp-kafka:7.9.3.arm64
  depends_on:
    kafka:
      condition: service_healthy
  entrypoint: [ "/bin/bash","-lc" ]
  command: >
    ...
    kafka-topics --bootstrap-server kafka:9092
      --create --if-not-exists --topic "${KAFKA_TOPIC:-events}" --partitions 3 ...
```

* Waits for Kafka to be usable, then creates the topic `${KAFKA_TOPIC:-events}` if needed.
* Runs once and exits with success.

### Spark Master

```yaml
spark-master:
  image: spark:3.5.1-python3
  command: ["spark-class","org.apache.spark.deploy.master.Master","--host","spark-master"]
  ports:
    - "7077:7077"  # Spark master RPC
    - "8080:8080"  # Spark master UI
  volumes:
    - ./data/output:/opt/spark-output
    - ./spark:/opt/spark-app
```

* Runs the Spark master at `spark://spark-master:7077`.
* Mounts:

  * `./spark` → `/opt/spark-app` (your `app.py` lives here).
  * `./data/output` → `/opt/spark-output` (where streaming job writes Parquet).

### Spark Worker

```yaml
spark-worker:
  image: spark:3.5.1-python3
  depends_on: [spark-master]
  command: ["spark-class","org.apache.spark.deploy.worker.Worker","spark://spark-master:7077"]
  environment:
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_MEMORY=2g
  ports:
    - "8081:8081"  # worker UI
  volumes:
    - ./data/output:/opt/spark-output
    - ./spark:/opt/spark-app
```

* Registers with the master.
* Shares the same code and output volume as the master, so executors can write to `/opt/spark-output`.

### Spark Streaming job container

```yaml
spark-streaming:
  image: spark:3.5.1-python3
  restart: unless-stopped
  depends_on:
    kafka: { condition: service_healthy }
    kafka-init: { condition: service_completed_successfully }
    spark-master: { condition: service_started }
  volumes:
    - ./spark:/opt/spark-app
    - ./data/output:/opt/spark-output
    - ./cache/ivy:/tmp/.ivy2
  entrypoint:
    - /bin/bash
    - -lc
    - >
      RUN_ID=$(date +%s);
      export RUN_ID;
      rm -rf /opt/spark-output/parquet/_chk || true;
      mkdir -p /tmp/.ivy2 && mkdir -p /opt/spark-output && chmod -R 777 /opt/spark-output;
      exec /opt/spark/bin/spark-submit
      --master spark://spark-master:7077
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
      --conf spark.sql.shuffle.partitions=4
      --conf spark.jars.ivy=/tmp/.ivy2
      /opt/spark-app/app.py
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    - KAFKA_TOPIC=${KAFKA_TOPIC:-events}
    - OUTPUT_PATH=/opt/spark-output/parquet
    - HOME=/tmp
```

* When the container starts, it:

  * Sets `RUN_ID` to current epoch time.
  * Cleans old checkpoint `_chk` for a fresh run.
  * Ensures `/opt/spark-output` is writable.
  * Runs `spark-submit` against the **cluster** (not local mode) with Kafka package.
* Your `app.py` uses env vars to know:

  * Which Kafka to read (`kafka:9092`).
  * Which topic (`events`).
  * Where to write Parquet (`/opt/spark-output/parquet`).

### Producer

```yaml
producer:
  build:
    context: ./producer
  restart: unless-stopped
  depends_on:
    kafka: { condition: service_healthy }
    kafka-init: { condition: service_completed_successfully }
  environment:
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    - KAFKA_TOPIC=${KAFKA_TOPIC:-events}
    - MSGS_PER_SEC=${MSGS_PER_SEC:-5}
  command: [ "python","-u","producer.py" ]
```

* Built from your custom `producer/Dockerfile`.
* Starts sending messages at `MSGS_PER_SEC` to Kafka as long as container is running.
* `-u` makes Python unbuffered → logs appear immediately in `docker logs producer`.

### Airflow

```yaml
airflow:
  build:
    context: ./airflow
    dockerfile: Dockerfile
  restart: unless-stopped
  depends_on:
    spark-streaming:
      condition: service_started
  environment:
    AIRFLOW__CORE__EXECUTOR: SequentialExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
    AIRFLOW__CORE__LOAD_EXAMPLES: "False"
    AIRFLOW__WEBSERVER__RBAC: "True"
    AIRFLOW__WEBSERVER__DEFAULT_USER_ROLE: Admin
    AIRFLOW__CORE__DEFAULT_TIMEZONE: "Asia/Kolkata"
    AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
  ports:
    - "8082:8080"
  volumes:
    - ./airflow/dags:/opt/airflow/dags
    - ./data/output:/opt/airflow/data
  command:
    - bash
    - -lc
    - |
      set -euo pipefail
      echo "=== [AIRFLOW] DB connection:"
      airflow config get-value database sql_alchemy_conn || true
      echo "=== [AIRFLOW] Initializing DB ==="
      airflow db init
      echo "=== [AIRFLOW] Pools BEFORE ==="
      airflow pools list || true
      echo "=== [AIRFLOW] Ensuring default_pool exists ==="
      airflow pools set default_pool "Default pool" 128 || true
      echo "=== [AIRFLOW] Pools AFTER ==="
      airflow pools list || true
      echo "=== [AIRFLOW] Creating admin user (if needed) ==="
      airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com || true
      echo "=== [AIRFLOW] Starting webserver & scheduler ==="
      airflow webserver &
      airflow scheduler
```

* Uses **SequentialExecutor** with SQLite DB → simple single-process execution, fine for demo.
* Mounts:

  * `./airflow/dags` → `/opt/airflow/dags` (your DAGs).
  * `./data/output` → `/opt/airflow/data` (so Airflow sees Parquet).
* Startup script:

  * Shows DB connection.
  * Initializes DB.
  * Ensures `default_pool` exists.
  * Creates admin user `admin` / `admin`.
  * Starts webserver and scheduler in the same container.

---

## 2. `.env`

```env
KAFKA_TOPIC=events
MSGS_PER_SEC=5
```

* Used by compose to parameterize:

  * Kafka init topic.
  * Producer message rate.

---

## 3. `spark/app.py` – Kafka → Structured Streaming → Parquet

Key parts:

```python
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "events")
output_path = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
run_id = os.getenv("RUN_ID", "local_run")
```

* Reads config from env.
* `RUN_ID` is set by the container entrypoint to current timestamp → each container start writes to a new `run_<RUN_ID>` folder.

### Spark session

```python
spark = (
    SparkSession.builder
    .appName("KafkaSparkStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
```

* Uses cluster master set by `spark-submit` (`--master spark://spark-master:7077`).

### Schema & Kafka source

```python
event_schema = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("ts", TimestampType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("source", StringType(), nullable=True),
    ]
)

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .load()
)
```

* Reads from topic `events` as a streaming DataFrame with Kafka’s default schema:

  * `key`, `value`, `topic`, `partition`, `offset`, `timestamp`, etc.

### JSON parsing

```python
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), event_schema).alias("data"))
    .select("data.*")
)
```

* Converts Kafka `value` (binary) → string → parse as JSON with your schema.
* Final columns: `id`, `ts`, `value`, `source`.

### Aggregation stream

```python
agg_df = (
    parsed_df
    .withWatermark("ts", "30 seconds")
    .groupBy(
        window(col("ts"), "10 seconds").alias("w"),
        col("id"),
    )
    .avg("value")
    .withColumnRenamed("avg(value)", "avg_value")
    .select(
        col("w.start").alias("window_start"),
        col("w.end").alias("window_end"),
        col("id"),
        col("avg_value"),
    )
)
```

* Sliding window aggregation:

  * 10-second windows on event-time column `ts`.
  * 30-second watermark → handles late data.
* Computes per-id average value per window.

### Console sink – for debugging

```python
console_query = (
    agg_df.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)
```

* Every ~10s, prints updated aggregates to the container logs.
* You can see it via `docker logs -f spark-streaming`.

### Parquet sink – raw data lake

```python
run_output_path = os.path.join(output_path, f"run_{run_id}")
checkpoint_path = os.path.join(run_output_path, "_chk")

parquet_query = (
    parsed_df.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", checkpoint_path)
    .option("path", run_output_path)
    .partitionBy("id")
    .trigger(processingTime="10 seconds")
    .start()
)
```

* Writes **raw parsed events** to `run_<RUN_ID>` subfolder:

  * Path inside container: `/opt/spark-output/parquet/run_<RUN_ID>`
  * Host: `./data/output/parquet/run_<RUN_ID>`
* Partitioned by `id` (creates folders like `id=0`, `id=1`, etc.).
* Uses checkpointing for exactly-once semantics.

---

## 4. Producer – `producer/producer.py` & Dockerfile

### Dockerfile

```dockerfile
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]
```

* Minimal Python image.
* `PYTHONUNBUFFERED=1` ensures logs stream directly.

### Producer logic

```python
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "events")
msgs_per_sec = env_int("MSGS_PER_SEC", 5)
```

* Configurable via env.

```python
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
)
```

* Uses `kafka-python` producer.
* Serializes Python dict → JSON bytes.

Main loop:

```python
while True:
    start = time.time()
    for _ in range(msgs_per_sec):
        event = {
            "id": random.randint(0, 4),
            "ts": datetime.now(timezone.utc).isoformat(),
            "value": round(random.uniform(0, 100), 3),
            "source": "sim-producer",
        }
        producer.send(topic, value=event)
    producer.flush()
    elapsed = time.time() - start
    sleep_for = max(0.0, 1.0 - elapsed)
    time.sleep(sleep_for)
```

* Every second:

  * Generates `MSGS_PER_SEC` events across IDs 0–4.
  * Each event matches the schema Spark expects.
* This keeps your streaming query fed indefinitely.

---

## 5. Airflow image – `airflow/Dockerfile`

```dockerfile
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

* Starts from official Airflow image.
* Installs Java 17 (you no longer strictly need it since the DAG uses pandas, but it’s harmless to keep).
* Installs Python dependencies:

  * `pandas`
  * `pyarrow`

---

## 6. Airflow DAG – `spark_parquet_validation_dag.py`

### DAG definition

```python
import pendulum
from pathlib import Path
from airflow.decorators import dag, task

DATA_BASE_PATH = Path("/opt/airflow/data/parquet")
```

* Points at the Parquet folder, which is the same physical directory Spark writes to (`./data/output/parquet`) mounted as `/opt/airflow/data/parquet`.

```python
@dag(
    dag_id="spark_parquet_validation_dag",
    description="Read Spark parquet output (written by PySpark) and log simple aggregations",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=pendulum.datetime(2025, 11, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["spark", "kafka", "validation"],
)
def spark_parquet_validation_dag():
    ...
```

* Runs every 5 minutes.
* No backfill (`catchup=False`).
* Uses TaskFlow API.

### Task: `aggregate_parquet`

```python
@task()
def aggregate_parquet():
    import pandas as pd

    if not DATA_BASE_PATH.exists():
        print(f"[aggregate_parquet] Path does not exist yet: {DATA_BASE_PATH}")
        ...
```

* Lazily imports pandas at runtime.
* If folder doesn’t exist yet, logs and exits gracefully.

Check for actual Parquet files:

```python
has_files = any(
    p.is_file() for p in DATA_BASE_PATH.rglob("*.parquet")
)
if not has_files:
    print(f"[aggregate_parquet] No .parquet files found under: {DATA_BASE_PATH}")
    return
```

* Protects against empty directory / no data scenario.

Read partitioned dataset:

```python
df = pd.read_parquet(DATA_BASE_PATH, engine="pyarrow")
```

* `pyarrow`’s engine automatically traverses partitioned subdirs (e.g., `run_<id>/id=0/part-...`).

Preview and validation:

```python
print("=== DataFrame head ===")
print(df.head(10))

if "id" not in df.columns or "value" not in df.columns:
    print("[aggregate_parquet] Expected columns 'id' and 'value' not found in dataframe.")
    print(f"Columns present: {list(df.columns)}")
    return
```

* Prints a sample to logs.
* Checks critical columns are present.

Aggregation:

```python
print("=== Count per id ===")
counts = df.groupby("id")["value"].agg(["count", "mean"]).reset_index()
print(counts)

print(f"[aggregate_parquet] Total rows: {len(df)}")
```

* Simple group-by to verify distribution of values per `id`.
* Total row count gives a quick sanity check that ingestion is happening.

Finally, the DAG object:

```python
aggregate_parquet()

dag = spark_parquet_validation_dag()
```

* Wire up the task inside the DAG and expose it to Airflow.

Run behavior:

* Every time the DAG fires, you can open:

  * Airflow UI → `spark_parquet_validation_dag`
    → Task `aggregate_parquet` → **Logs**
    to see DataFrame head, counts per ID, and total rows.

---