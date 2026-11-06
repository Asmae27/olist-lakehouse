from common.spark_session import get_spark_session

spark = get_spark_session("check-bronze")

df = spark.read.format("delta").load("s3a://bronze/orders/")
print(f"Lignes relues : {df.count()}")
df.printSchema()

spark.stop()
