# votes_enrichis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pathlib import Path

# -----------------------------
# 1️⃣ Chemins des datasets usage
# -----------------------------
VOTES_USAGE_PARQUET = Path("../data_lake/usage/votes")
DEPUTES_USAGE_PARQUET = Path("../data_lake/usage/depute")
ORGANES_USAGE_PARQUET = Path("../data_lake/usage/organes")

OUT_PARQUET = Path("../data_lake/usage/votes_enrichis")

# -----------------------------
# 2️⃣ Création de la SparkSession
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
#     acteurRef (vote) = uid (depute)
# -----------------------------
print("🔗 Jointure votes ⟷ députés (broadcast)...")
votes_dep = (
    votes.alias("v")
    .join(
        broadcast(deputes.alias("d")),
        col("v.acteurRef") == col("d.depute_uid"),
        "left"
    )
)

# -----------------------------
# 5️⃣ Jointure avec organes (groupe parlementaire)
#     organeRef_groupe (vote) = uid (organe)
# -----------------------------
print("🔗 Jointure votes_dep ⟷ organes (groupe)...")
votes_enrichis = (
    votes_dep.alias("vd")
    .join(
        broadcast(organes.alias("o")),
        col("vd.organeRef_groupe") == col("o.organe_uid"),
        "left"
    )
)

# -----------------------------
# 6️⃣ Sélection des colonnes finales
# -----------------------------
votes_enrichis = votes_enrichis.select(
    # Colonnes de base du vote
    col("v.uid_scrutin"),
    col("v.dateScrutin"),
    col("v.titre"),
    col("v.organeRef_groupe"),
    col("v.acteurRef"),
    col("v.mandatRef"),
    col("v.vote_position"),
    col("v.parDelegation"),
    col("v.numPlace"),

    # Infos député (préfixe depute_)
    col("d.prenom").alias("depute_prenom"),
    col("d.nom").alias("depute_nom"),
    col("d.profession").alias("depute_profession"),
    col("d.catSocPro").alias("depute_catSocPro"),
    col("d.uri_hatvp").alias("depute_uri_hatvp"),
    col("d.id_gp").alias("depute_id_gp"),
    col("d.id_par_pol").alias("depute_id_par_pol"),
    col("d.nb_mandats").alias("depute_nb_mandats"),

    # Infos organe de groupe (préfixe organe_)
    col("o.codeType").alias("organe_codeType"),
    col("o.libelle").alias("organe_libelle"),
    col("o.libelleAbrege").alias("organe_libelleAbrege"),
    col("o.libelleAbrev").alias("organe_libelleAbrev"),
    col("o.regime").alias("organe_regime"),
    col("o.legislature").alias("organe_legislature"),
    col("o.region").alias("organe_region"),
    col("o.departement").alias("organe_departement"),
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
spark.stop()
print("✅ votes_enrichis terminé.")

