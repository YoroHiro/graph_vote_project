import pandas as pd
import itertools
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict

# -------------------------------
# 1. Chargement des données
# -------------------------------
votes = pd.read_csv("../data_lake/formatted/votes_formatted.csv")
deputes = pd.read_csv("../data_lake/formatted/depute_formatted.csv")[["uid", "id_par_pol"]]

# on ajoute id_par_pol dans votes
votes = votes.merge(deputes, left_on="acteurRef", right_on="uid", how="left")
votes.drop(columns=["uid"], inplace=True)

# on garde seulement votes valides
votes = votes[votes["vote_position"].isin(["pour", "contre", "abstention"])]

# -------------------------------
# 2. Construction scrutin → votes
# -------------------------------
scrutin_dict = {}

for _, row in votes.iterrows():
    scr = row["uid_scrutin"]
    if scr not in scrutin_dict:
        scrutin_dict[scr] = {}
    scrutin_dict[scr][row["acteurRef"]] = row["vote_position"]

# dictionnaire acteur → parti
parti = dict(zip(deputes["uid"], deputes["id_par_pol"]))

# -------------------------------
# 3. Comptage co-votes
# -------------------------------
common_votes = defaultdict(int)
same_votes = defaultdict(int)

for scr, mapping in scrutin_dict.items():
    acteurs = list(mapping.keys())

    for a, b in itertools.combinations(acteurs, 2):
        vote_a = mapping[a]
        vote_b = mapping[b]

        common_votes[(a, b)] += 1

        if vote_a == vote_b:
            same_votes[(a, b)] += 1

# -------------------------------
# 4. Création du graphe G
# -------------------------------
G = nx.Graph()      #  ← GRAPHE CRÉÉ ICI (plus jamais erreur)

for (a, b), common in common_votes.items():
    if common == 0:
        continue

    ratio = same_votes[(a, b)] / common

    if ratio >= 0.8:                    # condition similarité
        if parti.get(a) != parti.get(b):  # condition partis ≠
            G.add_edge(a, b, weight=ratio)

print("Nombre d'arêtes :", len(G.edges()))
print("Nombre de nœuds :", len(G.nodes()))

# -------------------------------
# 5. Visualisation simple
# -------------------------------
plt.figure(figsize=(12, 12))

pos = nx.spring_layout(G, seed=42)  # positionnement simple

nx.draw(
    G,
    pos,
    with_labels=False,
    node_size=30,
    edge_color="grey",
    width=0.8
)

plt.title("Graphe des co-votes (≥ 50%) entre députés de partis différents")
plt.show()
