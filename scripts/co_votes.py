# co_votes.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, when
from pathlib import Path

# -----------------------------
# 0️⃣ Chemins
# -----------------------------
VOTES_ENRICHIS_PARQUET = Path("../data_lake/usage/votes_enrichis")
ORGANES_USAGE_PARQUET = Path("../data_lake/usage/organes")

OUT_PARQUET = Path("../data_lake/usage/co_votes")
OUT_TOP100_DIR = Path("../data_lake/usage/co_votes_csv/top100_interpartis")

# -----------------------------
# 1️⃣ SparkSession (avec un peu plus de mémoire)
# -----------------------------
spark = (
    SparkSession.builder
    .appName("CoVotes")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)

# -----------------------------
# 2️⃣ Lecture des données
# -----------------------------
print("📥 Lecture de votes_enrichis...")
votes = spark.read.parquet(str(VOTES_ENRICHIS_PARQUET))

print("📥 Lecture des organes (PARPOL)...")
parpol = (
    spark.read.parquet(str(ORGANES_USAGE_PARQUET))
    .filter(col("codeType") == "PARPOL")
    .select(
        col("uid").alias("parpol_uid"),
        col("libelle").alias("parpol_libelle"),
        col("libelleAbrev").alias("parpol_libelleAbrev"),
    )
)

# -----------------------------
# 3️⃣ Préparation des votes
#    - votes_all : pour compter nb_votes_tot (co-présence, toutes positions)
#    - votes_sim : pour nb_votes_semblables (même position)
# -----------------------------
votes_all = votes.select(
    "uid_scrutin",
    "acteurRef",
    "depute_nom",
    "depute_prenom",
    "depute_id_par_pol",
)

votes_sim = (
    votes
    .select(
        "uid_scrutin",
        "acteurRef",
        "vote_position",
        "depute_nom",
        "depute_prenom",
        "depute_id_par_pol",
    )
    .filter(col("vote_position").isin("pour", "contre", "abstention"))
)

# -----------------------------
# 4️⃣ nb_votes_tot : co-présence dans un même scrutin (toutes positions)
# -----------------------------
v1_all = votes_all.alias("v1_all")
v2_all = votes_all.alias("v2_all")

co_all = (
    v1_all.join(
        v2_all,
        (
            (col("v1_all.uid_scrutin") == col("v2_all.uid_scrutin")) &
            (col("v1_all.acteurRef") < col("v2_all.acteurRef"))
        ),
        "inner",
    )
)

co_all_agg = (
    co_all
    .groupBy(
        col("v1_all.acteurRef").alias("acteur1"),
        col("v2_all.acteurRef").alias("acteur2"),
    )
    .agg(count("*").alias("nb_votes_tot"))
)

# -----------------------------
# 5️⃣ nb_votes_semblables : même scrutin + même position
# -----------------------------
v1 = votes_sim.alias("v1")
v2 = votes_sim.alias("v2")

co_sim = (
    v1.join(
        v2,
        (
            (col("v1.uid_scrutin") == col("v2.uid_scrutin")) &
            (col("v1.vote_position") == col("v2.vote_position")) &
            (col("v1.acteurRef") < col("v2.acteurRef"))
        ),
        "inner",
    )
)

co_sim_agg = (
    co_sim
    .groupBy(
        col("v1.acteurRef").alias("acteur1"),
        col("v2.acteurRef").alias("acteur2"),
        col("v1.depute_nom").alias("depute1_nom"),
        col("v1.depute_prenom").alias("depute1_prenom"),
        col("v1.depute_id_par_pol").alias("parti1_id"),
        col("v2.depute_nom").alias("depute2_nom"),
        col("v2.depute_prenom").alias("depute2_prenom"),
        col("v2.depute_id_par_pol").alias("parti2_id"),
    )
    .agg(count("*").alias("nb_votes_semblables"))
)

# -----------------------------
# 6️⃣ Jointure nb_votes_semblables ⟷ nb_votes_tot
# -----------------------------
co_votes = co_sim_agg.join(
    co_all_agg,
    on=["acteur1", "acteur2"],
    how="left",
)

# -----------------------------
# 7️⃣ Colonne ratio
#     ratio_votes_semblables = nb_votes_semblables / nb_votes_tot
# -----------------------------
co_votes = co_votes.withColumn(
    "ratio_votes_semblables",
    when(col("nb_votes_tot") > 0,
         col("nb_votes_semblables") / col("nb_votes_tot")
         ).otherwise(None)
)

# -----------------------------
# 8️⃣ Jointure avec les partis (PARPOL) pour noms de parti
# -----------------------------

# Parti 1
co_votes = (
    co_votes
    .join(
        parpol
        .withColumnRenamed("parpol_uid", "parti1_uid_join")
        .withColumnRenamed("parpol_libelle", "parti1_libelle")
        .withColumnRenamed("parpol_libelleAbrev", "parti1_libelleAbrev"),
        col("parti1_id") == col("parti1_uid_join"),
        "left",
    )
    .drop("parti1_uid_join")
)

# Parti 2
co_votes = (
    co_votes
    .join(
        parpol
        .withColumnRenamed("parpol_uid", "parti2_uid_join")
        .withColumnRenamed("parpol_libelle", "parti2_libelle")
        .withColumnRenamed("parpol_libelleAbrev", "parti2_libelleAbrev"),
        col("parti2_id") == col("parti2_uid_join"),
        "left",
    )
    .drop("parti2_uid_join")
)

# -----------------------------
# 9️⃣ Réorganisation des colonnes (structure logique)
# -----------------------------
co_votes_final = co_votes.select(
    # Acteur 1
    "acteur1",
    "depute1_nom",
    "depute1_prenom",
    "parti1_id",
    "parti1_libelle",
    "parti1_libelleAbrev",

    # Acteur 2
    "acteur2",
    "depute2_nom",
    "depute2_prenom",
    "parti2_id",
    "parti2_libelle",
    "parti2_libelleAbrev",

    # Stats
    "nb_votes_semblables",
    "nb_votes_tot",
    "ratio_votes_semblables",
)

print("📄 Schéma final co_votes :")
co_votes_final.printSchema()

# -----------------------------
# 🔟 Sauvegarde complète en Parquet
# -----------------------------
OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
co_votes_final.write.mode("overwrite").parquet(str(OUT_PARQUET))
print(f"💾 co_votes sauvegardé en parquet dans : {OUT_PARQUET}")

# -----------------------------
# 1️⃣1️⃣ Top 100 interpartis en CSV
#       (uniquement si parti1_id != parti2_id)
# -----------------------------
top100 = (
    co_votes_final
    .filter(col("parti1_id").isNotNull() & col("parti2_id").isNotNull())
    .filter(col("parti1_id") != col("parti2_id"))
    .orderBy(desc("nb_votes_semblables"))
    .limit(200)
)

OUT_TOP100_DIR.mkdir(parents=True, exist_ok=True)

(
    top100
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("encoding", "UTF-8") 
    .csv(str(OUT_TOP100_DIR))
)


print(f"📄 Top 100 interpartis écrit dans le dossier : {OUT_TOP100_DIR}")

spark.stop()
print("✅ co_votes terminé.")
