# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F

# =============================
# Spark Session
# =============================
spark = (
    SparkSession.builder
    .appName("gold_datamart_top_products")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

# =============================
# Lecture Silver
# =============================
product_ranking = spark.table("silver.product_ranking")

# =============================
# Filtrage Top 5 produits par pays
# =============================
gold_top_products = (
    product_ranking
    .filter(F.col("rank") <= 5)
    .select(
        F.col("CountryName").alias("country"),
        F.col("ProductNo").alias("product_no"),
        F.col("ProductName").alias("product_name"),
        F.col("total_sales"),
        F.col("total_quantity"),
        F.col("rank")
    )
)

# =============================
# PostgreSQL JDBC config
# =============================
jdbc_url = "jdbc:postgresql://postgres-gold:5432/datamart"

jdbc_properties = {
    "user": "gold_user",
    "password": "gold_password",
    "driver": "org.postgresql.Driver"
}

# =============================
# Écriture Gold
# =============================
gold_top_products.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="gold_top_products",
        properties=jdbc_properties
    )




spark.stop()
