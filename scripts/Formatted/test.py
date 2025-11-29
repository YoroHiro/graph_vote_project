#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import itertools
import networkx as nx
import matplotlib.pyplot as plt
from collections import Counter
from pathlib import Path
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches

# ---------------------------------------------------------
# 0) Paramètres et fichiers
# ---------------------------------------------------------
FICHIER_VOTES = Path("../data_lake/formatted/votes_formatted.csv")
FICHIER_COVOTES = Path("../data_lake/usage/co_votes.csv")
FICHIER_DEPUTES_PARTIS = Path("../data_lake/usage/deputes_partis.csv")

RATIO_MIN = 0.9              # seuil pour créer une arête
MAX_EDGES_DISPLAY = 5000     # limite pour la visualisation


# ---------------------------------------------------------
# 1) Chargement des données
# ---------------------------------------------------------
print("🔹 Chargement des données...")

co_votes = pd.read_csv(FICHIER_COVOTES)  # same_votes
deputes_partis = pd.read_csv(FICHIER_DEPUTES_PARTIS)  # acteurRef → partiRef
votes = pd.read_csv(FICHIER_VOTES, usecols=["uid_scrutin", "acteurRef", "vote_position"])

votes = votes[votes["vote_position"].isin(["pour", "contre", "abstention"])]

print(f"  - co_votes : {len(co_votes)} lignes")
print(f"  - votes : {len(votes)} lignes")
print(f"  - députés_partis : {len(deputes_partis)} lignes")


# ---------------------------------------------------------
# 2) Calcul des votes communs
# ---------------------------------------------------------
print("🔹 Comptage des votes en commun (common_votes)...")

common_counter = Counter()

for scr, subset in votes.groupby("uid_scrutin"):
    acteurs = subset["acteurRef"].dropna().unique()
    for a1, a2 in itertools.combinations(sorted(acteurs), 2):
        common_counter[(a1, a2)] += 1

print(f"  - Paires avec votes communs : {len(common_counter)}")


# ---------------------------------------------------------
# 3) Préparation : same_votes depuis co_votes.csv
# ---------------------------------------------------------
same_counter = {}

for _, row in co_votes.iterrows():
    a1, a2, w = row["acteur1"], row["acteur2"], row["poids"]
    key = tuple(sorted((a1, a2)))
    same_counter[key] = same_counter.get(key, 0) + w

# mapping acteur → parti
parti = dict(zip(deputes_partis["acteurRef"], deputes_partis["partiRef"]))


# ---------------------------------------------------------
# 4) Construction du graphe G
# ---------------------------------------------------------
print(f"🔹 Construction du graphe (ratio ≥ {RATIO_MIN})...")

G = nx.Graph()

for (a, b), common in common_counter.items():
    same = same_counter.get((a, b), 0)

    if common == 0:
        continue

    ratio = same / common

    if ratio >= RATIO_MIN:
        # On ne garde que les liens interpartis
        if parti.get(a) != parti.get(b):
            G.add_edge(a, b, weight=ratio, same=same, common=common)

print("  - Nœuds :", len(G.nodes()))
print("  - Arêtes :", len(G.edges()))

if len(G.edges()) == 0:
    print("⚠ Aucun lien ne respecte le seuil.")
    exit()


# ---------------------------------------------------------
# 5) Réduction pour visualisation
# ---------------------------------------------------------
if len(G.edges()) > MAX_EDGES_DISPLAY:
    print(f"🔹 Réduction du graphe pour l'affichage (top {MAX_EDGES_DISPLAY} arêtes)...")

    edges_sorted = sorted(
        G.edges(data=True),
        key=lambda x: x[2].get("weight", 0),
        reverse=True
    )
    edges_top = edges_sorted[:MAX_EDGES_DISPLAY]

    H = nx.Graph()
    H.add_edges_from(edges_top)
else:
    print("🔹 Graphe assez léger, on affiche tout.")
    H = G

print("  - Nœuds affichés :", len(H.nodes()))
print("  - Arêtes affichées :", len(H.edges()))


# ---------------------------------------------------------
# 6) Couleurs des nœuds par parti politique
# ---------------------------------------------------------
print("🔹 Attribution des couleurs par parti politique...")

partis_uniques = sorted({parti.get(n, "Inconnu") for n in H.nodes()})

cmap = cm.get_cmap('tab20', len(partis_uniques))
parti_to_color = {p: mcolors.to_hex(cmap(i)) for i, p in enumerate(partis_uniques)}

node_colors = [parti_to_color.get(parti.get(n, "Inconnu"), "#000000")
               for n in H.nodes()]


# ---------------------------------------------------------
# 7) Visualisation (spring_layout + SciPy)
# ---------------------------------------------------------
print("🔹 Calcul du layout (spring_layout) ...")

plt.figure(figsize=(15, 15))

pos = nx.spring_layout(H, seed=42, weight="weight")  # utilise SciPy

weights = [d.get("weight", 0.0) for _, _, d in H.edges(data=True)]

# Draw nodes
nx.draw_networkx_nodes(
    H, pos,
    node_size=60,
    node_color=node_colors,
    alpha=0.9
)

# Draw edges
nx.draw_networkx_edges(
    H, pos,
    width=[w * 2 for w in weights],
    edge_color="grey",
    alpha=0.35
)

# Légende (max 12 partis)
max_legende = 12
patches = [
    mpatches.Patch(color=c, label=p)
    for i, (p, c) in enumerate(parti_to_color.items())
    if i < max_legende
]

plt.legend(handles=patches, title="Partis politiques", loc="lower left", fontsize=9)

plt.title(
    f"Graphe des co-votes interpartis (ratio ≥ {int(RATIO_MIN*100)}%)\n"
    f"Arêtes affichées : {len(H.edges())}"
)

plt.axis("off")
plt.tight_layout()
plt.show()


print("🏁 Fin du script.")
