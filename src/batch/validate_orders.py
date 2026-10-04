# src/batch/validate_orders.py
from common.spark_session import get_spark_session
from great_expectations.dataset import SparkDFDataset

spark = get_spark_session("validate-orders")

df = spark.read.format("delta").load("s3a://bronze/orders/")

# Valeurs réelles trouvées dans order_status, pas inventées
distinct_statuses = [row["order_status"] for row in df.select("order_status").distinct().collect()]
print(f"Valeurs distinctes de order_status : {distinct_statuses}")

ge_df = SparkDFDataset(df)

result_order_id = ge_df.expect_column_values_to_not_be_null("order_id")
result_status = ge_df.expect_column_values_to_be_in_set("order_status", distinct_statuses)
result_purchase_ts = ge_df.expect_column_values_to_not_be_null("order_purchase_timestamp")

for name, result in [
    ("order_id non null", result_order_id),
    ("order_status dans l'ensemble connu", result_status),
    ("order_purchase_timestamp non null", result_purchase_ts),
]:
    status = "PASS" if result.success else "FAIL"
    print(f"[{status}] {name} — {result.result.get('unexpected_count', 0)} ligne(s) en échec")

spark.stop()