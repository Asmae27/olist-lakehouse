from pyspark.sql import functions as F, Window

from batch.silver_quarantine import split_valid_and_quarantine
from common.spark_session import get_spark_session
from common.storage import ensure_buckets_exist

SOURCE = "s3a://bronze/orders_stream/"
SILVER = "s3a://silver/orders"                  # sans "/" final : utilisé dans du SQL
QUARANTINE = "s3a://quarantine/orders_stream/"

DATE_COLS = [
    "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
]
SILVER_COLS = ["order_id", "customer_id", "order_status"] + DATE_COLS


def malformed_rule(col_name):
    """Valide si on n'a PAS (une valeur présente ET un résultat nul après conversion)."""
    raw = f"_raw_{col_name}"
    return (
        f"{col_name}_malformed",
        lambda: ~(F.col(raw).isNotNull() & (F.trim(F.col(raw)) != "") & F.col(col_name).isNull()),
    )


def main():
    ensure_buckets_exist(["silver", "quarantine"])
    spark = get_spark_session("silver-orders-from-stream")

    bronze = spark.read.format("delta").load(SOURCE)
    n_bronze = bronze.count()

    # 1) Déduplication : on garde le message le plus récent par order_id.
    #    Les lignes sans order_id (message illisible) ont chacune leur propre groupe,
    #    sinon la fenêtre n'en garderait qu'une seule.
    w = Window.partitionBy("dedupe_key").orderBy(
        F.col("kafka_timestamp").desc(), F.col("kafka_offset").desc())
    deduped = (
        bronze
        .withColumn("dedupe_key", F.coalesce(
            F.col("order_id"),
            F.concat_ws(":", F.col("kafka_partition").cast("string"),
                        F.col("kafka_offset").cast("string"))))
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn", "dedupe_key")
    )
    n_dedup = deduped.count()

    # 2) Typage : on GARDE la valeur brute (_raw_...) avant de convertir.
    #    Chaîne vide -> null (valeur légitimement absente) ; chaîne non vide
    #    illisible -> null aussi, mais la règle "_malformed" la distinguera.
    typed = deduped
    for c in DATE_COLS:
        typed = typed.withColumn(f"_raw_{c}", F.col(c))
        typed = typed.withColumn(c, F.to_timestamp(F.when(F.trim(F.col(c)) != "", F.col(c))))

    # 3) Règles : celles d'orders + une règle "malformé" par colonne de date.
    df_valid, df_quarantine = split_valid_and_quarantine(
        typed, "orders", extra_rules=[malformed_rule(c) for c in DATE_COLS])
    df_valid = df_valid.select(*SILVER_COLS)

    n_valid = df_valid.count()
    n_quarantine = df_quarantine.count()
    print(f"Flux Bronze : {n_bronze} lignes, {n_dedup} après déduplication")
    print(f"Flux : {n_valid} valides, {n_quarantine} en quarantaine "
          f"(total vérifié : {n_valid + n_quarantine == n_dedup})")
    if n_valid + n_quarantine != n_dedup:
        raise RuntimeError(f"Réconciliation du flux échouée : {n_valid} + {n_quarantine} != {n_dedup}")

    # 4) Quarantaine recalculée à chaque exécution (fonction de tout le Bronze flux).
    (df_quarantine.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(QUARANTINE))

    # 5) Upsert dans Silver sans toucher aux lignes absentes du flux.
    before = spark.read.format("delta").load(SILVER).count()
    df_valid.createOrReplaceTempView("orders_updates")
    result = spark.sql(f"""
        MERGE INTO delta.`{SILVER}` AS t
        USING orders_updates AS s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"MERGE : {result.collect()[0].asDict()}")

    after_df = spark.read.format("delta").load(SILVER)
    after_count = after_df.count()
    after_distinct = after_df.select("order_id").distinct().count()
    print(f"Silver orders : {before} -> {after_count} lignes ({after_distinct} order_id distincts)")

    if after_count != after_distinct:
        raise RuntimeError("Doublons sur order_id dans silver/orders après le MERGE")
    if after_count < before:
        raise RuntimeError(f"silver/orders a perdu des lignes pendant le MERGE ({before} -> {after_count})")
    spark.stop()


if __name__ == "__main__":
    main()