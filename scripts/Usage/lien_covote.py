import pandas as pd
from itertools import combinations
from collections import Counter
from pathlib import Path

FICHIER_VOTES = Path("../data_lake/formatted/votes_formatted.csv")
FICHIER_SORTIE = Path("../data_lake/usage/co_votes.csv")

print("🔗 Construction des liens de co-vote...")

df = pd.read_csv(FICHIER_VOTES)
df = df[df["vote_position"].isin(["pour", "contre", "abstention"])]

edges_counter = Counter()

# Comptage des votes communs
for (uid, position), subset in df.groupby(["uid_scrutin", "vote_position"]):
    acteurs = subset["acteurRef"].dropna().unique()
    for a1, a2 in combinations(sorted(acteurs), 2):
        edges_counter[(a1, a2)] += 1

edges = [{"acteur1": a1, "acteur2": a2, "poids": w} for (a1, a2), w in edges_counter.items()]
edges_df = pd.DataFrame(edges)

edges_df.to_csv(FICHIER_SORTIE, index=False)
print(f"✅ Liens créés → {FICHIER_SORTIE}")
print(f"🔢 Nombre d’arêtes : {len(edges_df)}")
