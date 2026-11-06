from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common.spark_session import get_spark_session
from common.storage import ensure_buckets_exist

BROKER = "kafka:19092"
TOPIC = "olist_orders"
TARGET = "s3a://bronze/orders_stream/"
CHECKPOINT = "s3a://checkpoints/orders_stream/"

ORDER_FIELDS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
# Tout en texte : le producer envoie le CSV tel quel, Bronze reste fidèle à la source.
ORDER_SCHEMA = StructType([StructField(name, StringType(), True) for name in ORDER_FIELDS])


def main():
    ensure_buckets_exist(["bronze", "checkpoints"])
    spark = get_spark_session("orders-stream-to-bronze")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")  # ne sert qu'au PREMIER démarrage
        .load()
    )

    parsed = (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("data", F.from_json("raw_value", ORDER_SCHEMA))
        .select("data.*", "kafka_key", "raw_value", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("ingested_at", F.current_timestamp())
    )

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start(TARGET)
    )
    query.awaitTermination()

    result = spark.read.format("delta").load(TARGET)
    total = result.count()
    distinct = result.select("order_id").distinct().count()
    unparsed = result.filter(F.col("order_id").isNull()).count()
    print(f"Lignes dans la table : {total} (order_id distincts : {distinct}, non parsées : {unparsed})")
    spark.stop()


if __name__ == "__main__":
    main()
