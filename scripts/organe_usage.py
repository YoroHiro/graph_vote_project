# organe_usage.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pathlib import Path

# -------------------------------------------------
# 🔧 Chemin vers le CSV des organes
# -------------------------------------------------
CSV_PATH = Path("../data_lake/formatted/organes/organes_formatted.csv")

# -------------------------------------------------
# 1️⃣ SparkSession
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("OrganeUsage")
    .master("local[*]")
    .getOrCreate()
)

print("📥 Lecture du CSV des organes...")
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(CSV_PATH))
)

print("📄 Schéma complet du DataFrame initial :")
df_raw.printSchema()

print("🔎 Aperçu brut :")
df_raw.show(20, truncate=False)

# -------------------------------------------------
# 2️⃣ Colonnes nécessaires pour le workflow
#     uid = clé
#     codeType = GP ou PARPOL
#     libelle = nom complet
#     libelleAbrev = abréviation (ex : EPR, LFI-NFP, PCF)
# -------------------------------------------------
colonnes_cible = [
    "uid",
    "codeType",
    "libelle",
    "libelleAbrev",
]

df = df_raw.select(*colonnes_cible).dropDuplicates(["uid"])

print("\n📄 Schéma après sélection des colonnes usage :")
df.printSchema()

print("\n🔎 Aperçu usage :")
df.show(20, truncate=False)

# -------------------------------------------------
# 3️⃣ Quelques stats utiles (optionnel)
# -------------------------------------------------

print("\n📊 Nombre d'organes par codeType :")
(
    df.groupBy("codeType")
      .agg(count("*").alias("nb"))
      .orderBy(col("nb").desc())
      .show(truncate=False)
)

print("\n📊 Liste des libellés uniques par type :")
(
    df.groupBy("codeType", "libelle")
      .count()
      .orderBy("codeType", "libelle")
      .show(50, truncate=False)
)

# -------------------------------------------------
# 4️⃣ Sauvegarde en Parquet (layer usage)
# -------------------------------------------------
OUT_PARQUET = Path("../data_lake/usage/organes")
(
    df.write
      .mode("overwrite")
      .parquet(str(OUT_PARQUET))
)

print(f"\n💾 DataFrame organes (usage) sauvegardé en parquet dans : {OUT_PARQUET}")

spark.stop()
print("✅ organe_usage terminé.")
