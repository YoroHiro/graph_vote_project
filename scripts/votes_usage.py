# votes_usage.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path

# -------------------------------------------------
# 🔧 Chemin vers le CSV des votes formatés
# -------------------------------------------------
CSV_PATH = Path("../data_lake/formatted/vote/votes_formatted.csv")

# -------------------------------------------------
# 1️⃣ SparkSession
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("VotesUsage")
    .config("spark.driver.memory", "4g")
    .master("local[*]")
    .getOrCreate()
)

print("📥 Lecture du CSV des votes...")
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
# 2️⃣ Sélection des colonnes du modèle usage
# -------------------------------------------------
colonnes_cible = [
    "uid_scrutin",
    "dateScrutin",
    "titre",
    "organeRef_groupe",
    "acteurRef",
    "mandatRef",
    "vote_position",
    "parDelegation",
    "numPlace",
]

df = df_raw.select(*colonnes_cible)

print("\n📄 Schéma après sélection des colonnes usage :")
df.printSchema()

print("\n🔎 Aperçu usage :")
df.show(20, truncate=False)

# (Optionnel) Si tu veux filtrer que les votes exprimés :
# df = df.filter(col("vote_position").isin(["pour", "contre", "abstention"]))

# -------------------------------------------------
# 3️⃣ Sauvegarde en Parquet (layer usage)
# -------------------------------------------------
OUT_PARQUET = Path("../data_lake/usage/votes")

(
    df.write
      .mode("overwrite")
      .parquet(str(OUT_PARQUET))
)

print(f"\n💾 DataFrame votes (usage) sauvegardé en parquet dans : {OUT_PARQUET}")

spark.stop()
print("✅ votes_usage terminé.")
