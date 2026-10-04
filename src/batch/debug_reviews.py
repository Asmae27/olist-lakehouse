from common.spark_session import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType

spark = get_spark_session("debug-reviews")

schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("multiLine", "true")
    .schema(schema)
    .csv("/opt/spark/work-dir/data/raw/olist_order_reviews_dataset.csv")
)
print(f"Total lignes (multiLine, schéma explicite) : {df.count()}")

corrupt = df.filter(df["_corrupt_record"].isNotNull())
print(f"Lignes corrompues détectées : {corrupt.count()}")
corrupt.select("_corrupt_record").show(10, truncate=False)

spark.stop()