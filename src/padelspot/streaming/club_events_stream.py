from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


TOPIC = "padel_club_events"
PROJECT_ROOT = Path("/home/jovyan/work") if Path("/home/jovyan/work").exists() else Path.cwd()
STREAMING_ROOT = PROJECT_ROOT / "data" / "streaming"
BRONZE_PATH = STREAMING_ROOT / "bronze_club_events"
SILVER_PATH = STREAMING_ROOT / "silver_clubs_current"
GOLD_PATH = STREAMING_ROOT / "gold_clubs_by_department"
CHECKPOINT_PATH = STREAMING_ROOT / "checkpoints" / "club_events_stream"


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("club_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("department", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("courts", IntegerType(), True),
        StructField("source", StringType(), True),
        StructField("event_time", StringType(), True),
    ]
)


def _default_bootstrap_servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _spark() -> SparkSession:
    import pyspark

    spark_version = ".".join(pyspark.__version__.split(".")[:3])
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--packages org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version} pyspark-shell",
    )
    return (
        SparkSession.builder.appName("padelspot-club-events-stream")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _parse_kafka_messages(kafka_df: DataFrame) -> DataFrame:
    return (
        kafka_df.selectExpr("CAST(value AS STRING) AS raw_json", "timestamp AS kafka_timestamp")
        .withColumn("event", F.from_json("raw_json", EVENT_SCHEMA))
        .select("raw_json", "kafka_timestamp", "event.*")
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn("ingested_at", F.current_timestamp())
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("club_id").isNotNull())
        .filter(F.col("event_type").isin("club_created", "club_updated", "club_deleted"))
    )


def _write_current_state(batch_df: DataFrame) -> None:
    if not BRONZE_PATH.exists():
        return

    spark = batch_df.sparkSession
    bronze = spark.read.parquet(str(BRONZE_PATH))
    ordering = Window.partitionBy("club_id").orderBy(
        F.col("event_timestamp").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
        F.col("event_id").desc(),
    )

    current = (
        bronze.withColumn("rn", F.row_number().over(ordering))
        .filter(F.col("rn") == 1)
        .filter(F.col("event_type") != "club_deleted")
        .drop("rn")
    )

    current.write.mode("overwrite").parquet(str(SILVER_PATH))

    gold = current.groupBy("department").agg(
        F.countDistinct("club_id").alias("nb_active_clubs"),
        F.sum(F.coalesce(F.col("courts"), F.lit(0))).alias("nb_total_courts"),
        F.max("event_timestamp").alias("last_event_time"),
    )
    gold.write.mode("overwrite").parquet(str(GOLD_PATH))

    print(
        "Updated streaming tables: "
        f"events_batch={batch_df.count()}, "
        f"active_clubs={current.count()}, "
        f"departments={gold.count()}"
    )


def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no events")
        return

    print(f"Batch {batch_id}: received {batch_df.count()} valid events")
    batch_df.write.mode("append").parquet(str(BRONZE_PATH))
    _write_current_state(batch_df)


def run_stream(bootstrap_servers: str, topic: str, once: bool, duration: int | None) -> None:
    for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH, CHECKPOINT_PATH]:
        path.mkdir(parents=True, exist_ok=True)

    spark = _spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = _parse_kafka_messages(kafka_df)
    writer = (
        parsed.writeStream.foreachBatch(_process_batch)
        .option("checkpointLocation", str(CHECKPOINT_PATH))
        .queryName("padelspot_club_events")
    )

    query = writer.trigger(availableNow=True).start() if once else writer.start()
    print(f"Streaming from Kafka topic '{topic}' on {bootstrap_servers}")

    if duration is not None:
        query.awaitTermination(duration)
        query.stop()
    else:
        query.awaitTermination()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume Padel club events from Kafka with Spark.")
    parser.add_argument("--bootstrap-servers", default=_default_bootstrap_servers())
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--duration", type=int)
    return parser


def main() -> None:
    args = _parser().parse_args()
    run_stream(args.bootstrap_servers, args.topic, args.once, args.duration)


if __name__ == "__main__":
    main()
