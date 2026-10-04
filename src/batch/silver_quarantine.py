import boto3
from botocore.client import Config

from common.spark_session import get_spark_session
from pyspark.sql import functions as F

# Chaque règle est une fonction (lambda), pas une expression déjà construite.
# Elle n'est évaluée qu'une fois appelée, donc après que la SparkSession existe.
DATA_QUALITY_RULES = {
    "orders": [
        ("order_id_not_null", lambda: F.col("order_id").isNotNull()),
        ("purchase_timestamp_not_null", lambda: F.col("order_purchase_timestamp").isNotNull()),
    ],
    "order_reviews": [
        ("review_id_not_null", lambda: F.col("review_id").isNotNull()),
        ("order_id_not_null", lambda: F.col("order_id").isNotNull()),
        ("review_score_not_null", lambda: F.col("review_score").isNotNull()),
    ],
}


def ensure_buckets_exist(bucket_names):
    """Crée les buckets MinIO manquants avant toute écriture Spark.
    S3 (et MinIO) exige qu'un bucket existe avant d'y écrire quoi que ce
    soit — contrairement à un système de fichiers classique qui crée les
    dossiers manquants à la volée."""
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    for name in bucket_names:
        if name not in existing:
            s3.create_bucket(Bucket=name)
            print(f"Bucket créé : {name}")
        else:
            print(f"Bucket déjà présent : {name}")


def split_valid_and_quarantine(df, table_name: str):
    """Source unique de vérité : applique chaque règle nommée, construit
    is_valid et quarantine_reason à partir des MÊMES conditions utilisées
    pour les statistiques. Aucune règle n'est écrite deux fois."""
    rules = DATA_QUALITY_RULES[table_name]

    df_checked = df
    failure_reasons = []
    for rule_name, condition_fn in rules:
        condition = condition_fn()  # construite ici seulement, session déjà active
        flag_col = f"_fails_{rule_name}"
        df_checked = df_checked.withColumn(flag_col, ~condition)
        failure_reasons.append(
            F.when(F.col(flag_col), F.lit(rule_name))
        )

    df_checked = df_checked.withColumn(
        "quarantine_reason",
        F.array_except(F.array(*failure_reasons), F.array(F.lit(None).cast("string")))
    )
    df_checked = df_checked.withColumn("is_valid", F.size("quarantine_reason") == 0)

    flag_cols = [f"_fails_{name}" for name, _ in rules]
    df_checked = df_checked.drop(*flag_cols)

    df_valid = df_checked.filter("is_valid").drop("is_valid", "quarantine_reason")
    df_quarantine = df_checked.filter("NOT is_valid").drop("is_valid")

    return df_valid, df_quarantine


def process_table(spark, table_name: str):
    df = spark.read.format("delta").load(f"s3a://bronze/{table_name}/")
    total = df.count()

    df_valid, df_quarantine = split_valid_and_quarantine(df, table_name)
    valid_count = df_valid.count()
    quarantine_count = df_quarantine.count()

    print(f"{table_name} : {total} lignes Bronze -> "
          f"{valid_count} Silver, {quarantine_count} quarantaine "
          f"(total vérifié : {valid_count + quarantine_count == total})")

    df_valid.write.format("delta").mode("overwrite").save(f"s3a://silver/{table_name}/")
    df_quarantine.write.format("delta").mode("overwrite").save(f"s3a://quarantine/{table_name}/")


def main():
    ensure_buckets_exist(["silver", "quarantine"])
    spark = get_spark_session("silver-quarantine")
    for table_name in DATA_QUALITY_RULES:
        process_table(spark, table_name)
    spark.stop()


if __name__ == "__main__":
    main()