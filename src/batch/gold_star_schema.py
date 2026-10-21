import datetime

from common.spark_session import get_spark_session
from pyspark.sql import functions as F, Window


def read(spark, layer, table):
    return spark.read.format("delta").load(f"s3a://{layer}/{table}/")


def write_gold(df, name):
    df.write.format("delta").mode("overwrite").save(f"s3a://gold/{name}/")


def add_surrogate_key(df, natural_key, key_name):
    """Clé de substitution entière, déterministe : numérotation selon l'ordre
    de la clé naturelle (unique après dédoublonnage)."""
    df = df.withColumn(key_name, F.row_number().over(Window.orderBy(natural_key)).cast("long"))
    return df.select(key_name, *[c for c in df.columns if c != key_name])


def build_dim_customers(spark):
    df = (
        read(spark, "silver", "customers")
        .select("customer_id", "customer_unique_id", "customer_zip_code_prefix",
                "customer_city", "customer_state")
        .dropDuplicates(["customer_id"])
    )
    return add_surrogate_key(df, "customer_id", "customer_key")


def build_dim_products(spark):
    translation = read(spark, "bronze", "product_category_translation")
    df = (
        read(spark, "silver", "products")
        .dropDuplicates(["product_id"])
        .join(translation, "product_category_name", "left")
        .select("product_id", "product_category_name", "product_category_name_english",
                "product_weight_g", "product_length_cm", "product_height_cm",
                "product_width_cm", "product_photos_qty")
    )
    return add_surrogate_key(df, "product_id", "product_key")


def build_dim_sellers(spark):
    df = (
        read(spark, "silver", "sellers")
        .select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
        .dropDuplicates(["seller_id"])
    )
    return add_surrogate_key(df, "seller_id", "seller_key")


def build_dim_date(spark):
    """Dimension entièrement dérivée : une ligne par jour, bornes lues dans
    les données réelles (pas une plage arbitraire)."""
    bounds = (
        read(spark, "silver", "orders")
        .select(F.min(F.to_date("order_purchase_timestamp")).alias("d0"),
                F.max(F.to_date("order_purchase_timestamp")).alias("d1"))
        .first()
    )
    print(f"Plage de dates des commandes : {bounds['d0']} -> {bounds['d1']}")

    days = spark.range(1).select(
        F.explode(F.sequence(F.lit(bounds["d0"]), F.lit(bounds["d1"]),
                             F.expr("interval 1 day"))).alias("full_date")
    )
    return days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.dayofmonth("full_date").alias("day"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").alias("day_of_week"),  # 1 = dimanche
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )


def build_fact_order_items(spark):
    items = read(spark, "silver", "order_items")
    orders = read(spark, "silver", "orders").select(
        "order_id", "customer_id", "order_status", "order_purchase_timestamp")

    # Garde-fou de cardinalité : orders doit avoir 1 ligne par order_id,
    # sinon la jointure dupliquerait des lignes de faits.
    dups = orders.groupBy("order_id").count().filter("count > 1").count()
    assert dups == 0, f"{dups} order_id dupliqués dans silver/orders"

    # On relit les dimensions PERSISTÉES : les clés de la fact sont ainsi
    # garanties identiques à celles stockées dans gold/.
    dim_c = read(spark, "gold", "dim_customers").select("customer_key", "customer_id")
    dim_p = read(spark, "gold", "dim_products").select("product_key", "product_id")
    dim_s = read(spark, "gold", "dim_sellers").select("seller_key", "seller_id")

    return (
        items.join(orders, "order_id", "left")
        .withColumn("purchase_date_key",
                    F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
        .join(dim_c, "customer_id", "left")
        .join(dim_p, "product_id", "left")
        .join(dim_s, "seller_id", "left")
        .select("order_id", "order_item_id", "customer_key", "product_key",
                "seller_key", "purchase_date_key", "order_status",
                "price", "freight_value")
    )


def validate(spark):
    fact = read(spark, "gold", "fact_order_items")
    n_fact = fact.count()
    n_src = read(spark, "silver", "order_items").count()
    print(f"fact_order_items : {n_fact} lignes vs silver/order_items : {n_src} "
          f"(égalité : {n_fact == n_src})")

    checks = [("customer_key", "dim_customers"), ("product_key", "dim_products"),
              ("seller_key", "dim_sellers"), ("purchase_date_key", "dim_date")]
    for key, dim in checks:
        dim_key = "date_key" if dim == "dim_date" else key
        dim_df = read(spark, "gold", dim).select(F.col(dim_key).alias(key))
        orphans = fact.join(dim_df, key, "left_anti").count()  # les clés nulles comptent aussi
        print(f"Orphelins {key} -> {dim} : {orphans}")


def main():
    spark = get_spark_session("gold-star-schema")
    write_gold(build_dim_customers(spark), "dim_customers")
    write_gold(build_dim_products(spark), "dim_products")
    write_gold(build_dim_sellers(spark), "dim_sellers")
    write_gold(build_dim_date(spark), "dim_date")
    write_gold(build_fact_order_items(spark), "fact_order_items")
    validate(spark)
    spark.stop()


if __name__ == "__main__":
    main()