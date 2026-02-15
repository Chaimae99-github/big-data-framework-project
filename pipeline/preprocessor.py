
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# =============================
# Spark Session avec Hive
# =============================
spark = (
    SparkSession.builder
    .appName("silver_processor")
    .enableHiveSupport()
    .getOrCreate()
)


# =============================
# Base SILVER (OBLIGATOIRE)
# =============================
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("USE silver")
# =============================


# LECTURE BRONZE (RAW)
# =============================
transactions = spark.read.parquet("hdfs://namenode:9000/raw/transactions")
customers    = spark.read.parquet("hdfs://namenode:9000/raw/customers")
products     = spark.read.parquet("hdfs://namenode:9000/raw/products")
countries    = spark.read.parquet("hdfs://namenode:9000/raw/countries")

# =============================
# DIM COUNTRIES (SILVER)
# =============================
dim_countries = (
    countries
    .dropDuplicates(["CountryID"])                     # Règle 1 : déduplication
    .dropna(subset=["CountryID", "CountryName"])       # Règle 2 : champs obligatoires
    .select("CountryID", "CountryName", "year", "month", "day")
)

dim_countries.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .saveAsTable("silver.dim_countries")

# =============================
# DIM CUSTOMERS (SILVER)
# =============================
dim_customers = (
    customers
    .dropDuplicates(["CustomerNo"])                    # Règle 3
    .dropna(subset=["CustomerNo", "Email"])            # Règle 4
    .select(
        "CustomerNo",          # STRING
        "FirstName",
        "LastName",
        "Email",
        "CountryID",
        "year", "month", "day"
    )
)

dim_customers.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .saveAsTable("silver.dim_customers")

# =============================
# DIM PRODUCTS (SILVER)
# =============================
dim_products = (
    products
    .dropDuplicates(["ProductNo"])                     # Règle 5
    .dropna(subset=["ProductNo", "ProductName"])
    .select(
        "ProductNo",          # STRING
        "ProductName",
        "year", "month", "day"
    )
)

dim_products.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .saveAsTable("silver.dim_products")

# =============================
# FACT TRANSACTIONS (SILVER)
# =============================
fact_transactions = (
    transactions
    # Typage (ProductNo, CustomerNo, TransactionNo restent STRING)
    .withColumn("Price", F.col("Price").cast("double"))
    .withColumn("Quantity", F.col("Quantity").cast("int"))

    # Règles de validation
    .dropna(subset=["TransactionNo", "CustomerNo", "ProductNo"])
    .filter(F.col("Quantity") > 0)                     # Règle 6
    .filter(F.col("Price") > 0)                        # Règle 7

    .select(
        "TransactionNo",       # STRING
        "Date",
        "ProductNo",
        "CustomerNo",
        "Quantity",
        "Price",
        "year", "month", "day"
    )
)

fact_transactions.write.mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .saveAsTable("silver.fact_transactions")

# =============================
# ENRICHISSEMENT (JOINTURES)
# =============================
fact_enriched = (
    fact_transactions
    .join(dim_customers, "CustomerNo", "left")
    .join(dim_countries, "CountryID", "left")
    .join(dim_products, "ProductNo", "left")
)

# cache OBLIGATOIRE (visible Spark UI)
fact_enriched.cache()

# =============================
# AGRÉGATION : CA PAR PRODUIT
# =============================
sales_by_product = (
    fact_enriched
    .groupBy("CountryName", "ProductNo", "ProductName")
    .agg(
        F.sum(F.col("Quantity") * F.col("Price")).alias("total_sales"),
        F.sum("Quantity").alias("total_quantity")
    )
)

# =============================
# WINDOW FUNCTION : RANK PRODUITS PAR PAYS
# =============================
window_spec = Window.partitionBy("CountryName").orderBy(F.col("total_sales").desc())

product_ranking = (
    sales_by_product
    .withColumn("rank", F.rank().over(window_spec))
)

# =============================
# ÉCRITURE SILVER ANALYTIQUE
# =============================
# product_ranking.write.mode("overwrite") \
#     .saveAsTable("silver.product_ranking")


# =============================
# INSERT DANS TABLE HIVE EXISTANTE
# =============================

product_ranking_select = product_ranking.select(
    "CountryName",
    "ProductNo",
    "ProductName",
    "total_sales",
    "total_quantity",
    "rank"
)

product_ranking_select.write \
    .mode("overwrite") \
    .insertInto("silver.product_ranking")











spark.stop()

