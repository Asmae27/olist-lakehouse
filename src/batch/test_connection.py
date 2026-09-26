from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test-connection-minio")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

data = [(1, "commande_test_1"), (2, "commande_test_2")]
df = spark.createDataFrame(data, ["id", "description"])

df.write.mode("overwrite").parquet("s3a://bronze/test/")

print("Écriture réussie.")
spark.stop()