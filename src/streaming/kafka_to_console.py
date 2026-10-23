from common.spark_session import get_spark_session
from pyspark.sql import functions as F

BROKER = "kafka:19092"
TOPIC = "olist_orders"

spark = get_spark_session("kafka-to-console")


def kafka_options(reader):
    return (
        reader.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
    )


# 1) Contrôle en lecture batch : combien de messages par partition ?
#    (en batch, le Kafka source lit par défaut de earliest à latest)
batch_df = kafka_options(spark.read).load()
print("Messages par partition (lecture batch) :")
batch_df.groupBy("partition").count().orderBy("partition").show()

# 2) La même source en streaming
stream_df = kafka_options(spark.readStream).option("startingOffsets", "earliest").load()

out = stream_df.select(
    F.col("key").cast("string").alias("order_id"),
    F.col("value").cast("string").alias("value"),
    "partition",
    "offset",
    "timestamp",
)

query = (
    out.writeStream
    .format("console")
    .option("numRows", 100)        # 20 par défaut : on veut voir les 100 messages
    .option("truncate", "false")
    .trigger(availableNow=True)    # traite tout ce qui est disponible, puis s'arrête
    .start()
)
query.awaitTermination()
spark.stop()