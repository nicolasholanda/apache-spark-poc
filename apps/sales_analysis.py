from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg

spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

df = spark.read.csv("/opt/spark-data/sales.csv", header=True, inferSchema=True)

print("\n=== Original Data ===")
df.show()

revenue_by_product = df.groupBy("product").agg(
    _sum(col("quantity") * col("price")).alias("total_revenue"),
    _sum("quantity").alias("total_quantity")
).orderBy(col("total_revenue").desc())

print("\n=== Revenue by Product ===")
revenue_by_product.show()
revenue_by_product.write.csv("/opt/spark-data/output/revenue_by_product", header=True, mode="overwrite")

revenue_by_region = df.groupBy("region").agg(
    _sum(col("quantity") * col("price")).alias("total_revenue"),
    count("*").alias("num_transactions")
).orderBy(col("total_revenue").desc())

print("\n=== Revenue by Region ===")
revenue_by_region.show()
revenue_by_region.write.csv("/opt/spark-data/output/revenue_by_region", header=True, mode="overwrite")

spark.stop()
