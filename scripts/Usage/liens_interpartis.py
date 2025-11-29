import pandas as pd

# === Chargement des données ===
deputes = pd.read_csv("../data_lake/usage/deputes_partis.csv")
liens = pd.read_csv("../data_lake/usage/co_votes.csv")

# Vérification de la structure
print(f"📘 Députés : {len(deputes)} lignes | Colonnes : {deputes.columns.tolist()}")
print(f"📗 Liens de co-vote : {len(liens)} lignes | Colonnes : {liens.columns.tolist()}")

# === Ajout des partis à chaque acteur ===
liens = (
    liens.merge(deputes, left_on="acteur1", right_on="acteurRef", how="left")
         .rename(columns={"partiRef": "parti1"})
         .drop(columns=["acteurRef"], errors="ignore")
)

liens = (
    liens.merge(deputes, left_on="acteur2", right_on="acteurRef", how="left")
         .rename(columns={"partiRef": "parti2"})
         .drop(columns=["acteurRef"], errors="ignore")
)

print(f"✅ Après fusion : {len(liens)} liens")

# === Filtrer : partis différents ===
liens_diff = liens[liens["parti1"] != liens["parti2"]]
print(f"🔀 Liens interpartis : {len(liens_diff)}")

# === Enregistrement complet (plus de limite de 100) ===
liens_diff = liens_diff.sort_values("poids", ascending=False)
liens_diff.to_csv("../data_lake/formatted/liens_interpartis.csv", index=False)

print(f"💾 Fichier sauvegardé : ../data_lake/formatted/liens_interpartis.csv")
print(f"🔢 Nombre total de liens interpartis : {len(liens_diff)}")
print("🏁 Terminé !")
