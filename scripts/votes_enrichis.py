# votes_enrichis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from datetime import datetime
from pathlib import Path

# -----------------------------
# 1️⃣ Chemins des datasets usage
# -----------------------------
VOTES_USAGE_PARQUET = Path("../data_lake/usage/votes")
DEPUTES_USAGE_PARQUET = Path("../data_lake/usage/depute")
ORGANES_USAGE_PARQUET = Path("../data_lake/usage/organes")

OUT_PARQUET = Path("../data_lake/usage/votes_enrichis")

# Dossier daté : AAAAMMJJ
date_str = datetime.now().strftime("%Y%m%d")
CSV_DIR = Path(f"../data_lake/usage/votes_enrichis_csv/{date_str}")
CSV_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# 2️⃣ SparkSession
# -----------------------------
spark = (
    SparkSession.builder
    .appName("VotesEnrichis")
    .master("local[*]")
    .getOrCreate()
)

# -----------------------------
# 3️⃣ Lecture des données usage
# -----------------------------
print("📥 Lecture des votes (usage)...")
votes = spark.read.parquet(str(VOTES_USAGE_PARQUET))

print("📥 Lecture des députés (usage)...")
deputes = (
    spark.read.parquet(str(DEPUTES_USAGE_PARQUET))
    .withColumnRenamed("uid", "depute_uid")
)

print("📥 Lecture des organes (usage)...")
organes = (
    spark.read.parquet(str(ORGANES_USAGE_PARQUET))
    .withColumnRenamed("uid", "organe_uid")
)

print("📄 Schéma votes :")
votes.printSchema()
print("📄 Schéma deputes :")
deputes.printSchema()
print("📄 Schéma organes :")
organes.printSchema()

# -----------------------------
# 4️⃣ Jointure votes ⟷ députés
# -----------------------------
print("🔗 Jointure votes ⟷ députés (broadcast)...")
votes_dep = (
    votes
    .join(
        broadcast(deputes),
        votes.acteurRef == deputes.depute_uid,
        "left"
    )
)

# -----------------------------
# 5️⃣ Jointure avec organes (groupe parlementaire)
# -----------------------------
print("🔗 Jointure votes_dep ⟷ organes (groupe)...")
votes_enrichis = (
    votes_dep
    .join(
        broadcast(organes),
        votes_dep.organeRef_groupe == organes.organe_uid,
        "left"
    )
)

# -----------------------------
# 6️⃣ Sélection des colonnes finales
# -----------------------------
votes_enrichis = votes_enrichis.select(
    # Colonnes de base du vote
    col("uid_scrutin"),
    col("dateScrutin"),
    col("titre"),
    col("organeRef_groupe"),
    col("acteurRef"),
    col("mandatRef"),
    col("vote_position"),
    col("parDelegation"),
    col("numPlace"),

    # Infos député (préfixe depute_)
    col("prenom").alias("depute_prenom"),
    col("nom").alias("depute_nom"),
    col("profession").alias("depute_profession"),
    col("catSocPro").alias("depute_catSocPro"),
    col("uri_hatvp").alias("depute_uri_hatvp"),
    col("id_gp").alias("depute_id_gp"),
    col("id_par_pol").alias("depute_id_par_pol"),
    col("nb_mandats").alias("depute_nb_mandats"),

    # Infos organe (préfixe organe_)
    col("codeType").alias("organe_codeType"),
    col("libelle").alias("organe_libelle"),
    col("libelleAbrev").alias("organe_libelleAbrev"),
)

print("📄 Schéma votes_enrichis :")
votes_enrichis.printSchema()

print("🔎 Aperçu des premières lignes de votes_enrichis :")
votes_enrichis.show(20, truncate=False)

# -----------------------------
# 7️⃣ Sauvegarde en Parquet
# -----------------------------
OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

(
    votes_enrichis
    .write
    .mode("overwrite")
    .parquet(str(OUT_PARQUET))
)

print(f"💾 votes_enrichis sauvegardé dans : {OUT_PARQUET}")

# -----------------------------
# 8️⃣ Export CSV daté (diagnostic)
# -----------------------------
from datetime import datetime

# Dossier daté au format AAAAMMJJ
date_str = datetime.now().strftime("%Y%m%d")
CSV_DIR = Path(f"../data_lake/usage/votes_enrichis_csv/{date_str}")
CSV_DIR.mkdir(parents=True, exist_ok=True)

# a) Sample CSV
print("📤 Export d’un échantillon CSV (5000 lignes)...")
(
    votes_enrichis
    .limit(5000)
    .coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv(str(CSV_DIR / "sample"))
)

# b) Export complet CSV
print("📤 Export CSV complet (peut être lourd)...")
(
    votes_enrichis
    .coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv(str(CSV_DIR / "full"))
)

print(f"📁 CSV exportés dans : {CSV_DIR}")

spark.stop()
print("✅ votes_enrichis terminé.")
