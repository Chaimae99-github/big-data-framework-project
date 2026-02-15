# -*- coding: utf-8 -*-

from datetime import date
from pyspark.sql import SparkSession, functions as F
import time

# ======================
# Spark Session
# ======================
spark = (
    SparkSession.builder
    .appName("feeder")
    .getOrCreate()
)

today = date.today()

# ======================
# Sources + séparateurs
# ======================
sources = {
    "transactions": {
        "path": "file:///source/transactions.csv",
        "sep": ","
    },
    "products": {
        "path": "file:///source/products.csv",
        "sep": ","
    },
    "customers": {
        "path": "file:///source/customers.csv",
        "sep": ";"
    },
    "countries": {
        "path": "file:///source/countries.csv",
        "sep": ";"
    }
}

# ======================
# HDFS RAW base
# ======================
raw_base = "hdfs://namenode:9000/raw"

# ======================
# Ingestion
# ======================
for name, config in sources.items():

    print("Ingesting {}".format(name))

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", config["sep"])
        .csv(config["path"])
    )

    # Ajout partition ingestion
    df2 = (
        df.withColumn("year", F.lit(today.year))
          .withColumn("month", F.lit(today.month))
          .withColumn("day", F.lit(today.day))
    )

    df2.cache()

    time.sleep(5)

    output_path = "{}/{}".format(raw_base, name)

    (
        df2.repartition(4)
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )
    
    print("{} ingested".format(name))



spark.stop()
