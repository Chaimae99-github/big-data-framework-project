from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("gold_sales_country_year") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# =============================
# READ SILVER TABLES
# =============================
fact = spark.table("silver.fact_transactions")
customers = spark.table("silver.dim_customers")
countries = spark.table("silver.dim_countries")

# =============================
# FIX DATE FORMAT (VERY IMPORTANT)
# =============================
fact = fact.withColumn(
    "parsed_date",
    F.to_date("date", "M/d/yyyy")
)

# =============================
# BUILD DATAMART
# =============================
gold_sales = (
    fact.alias("f")
    .join(customers.alias("c"), F.col("f.customerno") == F.col("c.customerno"), "left")
    .join(countries.alias("co"), F.col("c.countryid") == F.col("co.countryid"), "left")
    .filter(F.col("co.countryname").isNotNull())
    .groupBy(
        F.year("parsed_date").alias("year"),
        F.col("co.countryname").alias("country")
    )
    .agg(
        F.sum(F.col("f.quantity") * F.col("f.price")).alias("total_sales"),
        F.sum("f.quantity").alias("total_quantity")
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

gold_sales.write \
    .mode("overwrite") \
    .jdbc(jdbc_url, "gold_sales_country_year", properties=jdbc_properties)

spark.stop()
