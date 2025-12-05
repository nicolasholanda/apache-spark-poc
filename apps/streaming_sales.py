from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("SalesStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("region", StringType(), True)
])

streaming_df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv("/opt/spark-data/streaming-input")

sales_with_revenue = streaming_df \
    .withColumn("revenue", col("quantity") * col("price")) \
    .withColumn("timestamp", current_timestamp())

revenue_by_product = sales_with_revenue \
    .groupBy("product") \
    .agg(_sum("revenue").alias("total_revenue"), _sum("quantity").alias("total_quantity"))

query = revenue_by_product \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

print("\n=== Streaming started! Drop CSV files into data/streaming-input/ ===\n")

query.awaitTermination()
