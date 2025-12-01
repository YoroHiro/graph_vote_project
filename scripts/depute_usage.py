# depute_usage.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pathlib import Path

spark = (
    SparkSession.builder
    .appName("DeputeUsage")
    .master("local[*]")
    .getOrCreate()
)

INPUT = "../data_lake/formatted/depute/depute_formatted.csv"
OUTPUT = "../data_lake/usage/depute"

print("📥 Lecture des députés...")
df = (
    spark.read.option("header", "true").option("inferSchema", "true")
    .csv(INPUT)
)

df = (
    df.withColumn("prenom", trim(col("prenom")))
      .withColumn("nom", trim(col("nom")))
      .withColumn("profession", trim(col("profession")))
)

print("💾 Sauvegarde en parquet...")
df.write.mode("overwrite").parquet(OUTPUT)

print("✅ Terminé — depute_usage")
spark.stop()
