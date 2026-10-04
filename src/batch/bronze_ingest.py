from common.spark_session import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType

TABLES = {
    "olist_orders_dataset.csv": "orders",
    "olist_customers_dataset.csv": "customers",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_translation",
}

DATA_DIR = "/opt/spark/work-dir/data/raw"

# Schéma explicite requis pour capturer les lignes corrompues via _corrupt_record.
# Nécessaire uniquement pour order_reviews, dont les champs texte libres
# (commentaires clients) contiennent des guillemets internes doublés et des
# retours à la ligne, conformes à la norme CSV mais mal gérés sans ces options.
ORDER_REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])


def ingest_standard_csv(spark, csv_filename: str, table_name: str) -> int:
    """Lit un CSV standard (sans champs texte libre complexes) et l'écrit en
    Delta dans bronze/<table_name>/. Retourne le nombre de lignes écrites."""
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{DATA_DIR}/{csv_filename}")
    )
    row_count = df.count()
    df.write.format("delta").mode("overwrite").save(f"s3a://bronze/{table_name}/")
    return row_count


def ingest_order_reviews(spark, csv_filename: str, table_name: str) -> int:
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .schema(ORDER_REVIEWS_SCHEMA)
        .csv(f"{DATA_DIR}/{csv_filename}")
    )

    total_count = df.count()
    print(f"  [DEBUG] Total lignes lues (avant filtrage) : {total_count}")

    corrupt_count = df.filter(df["_corrupt_record"].isNotNull()).count()
    print(f"  [DEBUG] Lignes corrompues détectées : {corrupt_count}")

    clean_df = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")
    row_count = clean_df.count()
    print(f"  [DEBUG] Lignes propres (à écrire) : {row_count}")

    clean_df.write.format("delta").mode("overwrite").save(f"s3a://bronze/{table_name}/")
    return row_count


def main():
    spark = get_spark_session("bronze-ingest")

    for csv_filename, table_name in TABLES.items():
        print(f"Ingestion : {csv_filename} -> bronze/{table_name}/")

        if table_name == "order_reviews":
            count = ingest_order_reviews(spark, csv_filename, table_name)
        else:
            count = ingest_standard_csv(spark, csv_filename, table_name)

        print(f"  {count} lignes écrites dans bronze/{table_name}/")

    spark.stop()


if __name__ == "__main__":
    main()