from common.spark_session import get_spark_session

spark = get_spark_session("test-connection-minio")


data = [(1, "commande_test_1"), (2, "commande_test_2")]
df = spark.createDataFrame(data, ["id", "description"])

df.write.mode("overwrite").parquet("s3a://bronze/test/")

print("Écriture réussie.")
spark.stop()