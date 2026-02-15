from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("gold_customer_lifetime_value") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# =============================
# READ SILVER TABLES
# =============================
fact = spark.table("silver.fact_transactions")
customers = spark.table("silver.dim_customers")

# =============================
# BUILD DATAMART
# =============================
gold_clv = (
    fact.alias("f")
    .join(customers.alias("c"),
          F.col("f.customerno") == F.col("c.customerno"),
          "left")
    .groupBy(
        F.col("f.customerno").alias("customer_no")
    )
    .agg(
        F.countDistinct("f.transactionno").alias("total_orders"),
        F.sum("f.quantity").alias("total_quantity"),
        F.sum(F.col("f.quantity") * F.col("f.price")).alias("total_sales")
    )
)

# =============================
# WRITE TO POSTGRES
# =============================
jdbc_url = "jdbc:postgresql://postgres-gold:5432/datamart"

jdbc_properties = {
    "user": "gold_user",
    "password": "gold_password",
    "driver": "org.postgresql.Driver"
}

gold_clv.write \
    .mode("overwrite") \
    .jdbc(jdbc_url, "gold_customer_lifetime_value", properties=jdbc_properties)

spark.stop()
