# spark/app.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)
from pyspark.sql.functions import from_json, col, window

def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "events")
    output_path = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
    run_id = os.getenv("RUN_ID", "local_run")

    print(f"Starting Spark streaming job. Topic={topic}, Kafka={kafka_bootstrap}")
    print(f"Output path: {output_path}, run_id={run_id}")

    spark = (
        SparkSession.builder
        .appName("KafkaSparkStreaming")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Schema of JSON value produced by producer
    event_schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("ts", TimestampType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
            StructField("source", StringType(), nullable=True),
        ]
    )

    # Read from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON payload
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), event_schema).alias("data"))
        .select("data.*")
    )

    # Simple window aggregation (10 sec windows, 30 sec watermark)
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

    # Console sink for debugging
    console_query = (
        agg_df.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Parquet sink: raw parsed events
    run_output_path = os.path.join(output_path, f"run_{run_id}")
    checkpoint_path = os.path.join(run_output_path, "_chk")

    print(f"Writing parquet to: {run_output_path}")
    print(f"Checkpoint location: {checkpoint_path}")

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

    # Wait for both queries
    console_query.awaitTermination()
    parquet_query.awaitTermination()


if __name__ == "__main__":
    main()
