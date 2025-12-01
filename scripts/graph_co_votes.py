# graph_co_votes_interactive.py

import pandas as pd
import networkx as nx
from pyvis.network import Network
from pathlib import Path

# -----------------------------
# 1️⃣ Paramètres & chemins
# -----------------------------
RATIO_MIN = 0.55  # seuil d'affichage des arêtes

CO_VOTES_DIR = Path("../data_lake/usage/co_votes_csv/top100_interpartis")
OUT_HTML = Path("../figures/graphe_co_votes_interactif.html")
OUT_HTML.parent.mkdir(parents=True, exist_ok=True)

# -----------------------------
# 2️⃣ Lecture du CSV co_votes
# -----------------------------
# On prend le premier CSV dans le dossier top100_interpartis
csv_files = sorted(CO_VOTES_DIR.glob("*.csv"))
if not csv_files:
    raise FileNotFoundError(f"Aucun CSV trouvé dans {CO_VOTES_DIR}")

csv_file = csv_files[0]
print(f"📄 Fichier utilisé : {csv_file}")

df = pd.read_csv(csv_file, encoding="utf-8")

# On enlève d'éventuelles colonnes 'Unnamed: ...'
df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

print(f"📥 Nombre de lignes initial : {len(df)}")

# Detecter la colonne ratio (ex: 'ratio_votes_semblables')
ratio_cols = [c for c in df.columns if "ratio" in c.lower()]
if not ratio_cols:
    raise ValueError("Aucune colonne contenant 'ratio' trouvée dans le CSV.")
ratio_col = ratio_cols[0]
print(f"📊 Colonne ratio utilisée : {ratio_col}")

# Filtre sur le ratio
df = df[df[ratio_col] >= RATIO_MIN].copy()
print(f"📉 Lignes après filtre ratio >= {RATIO_MIN} : {len(df)}")

# Colonnes attendues pour les libellés
required_cols = [
    "acteur1", "acteur2",
    "depute1_nom", "depute1_prenom", "parti1_libelleAbrev",
    "depute2_nom", "depute2_prenom", "parti2_libelleAbrev"
]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"Colonne manquante dans le CSV : {col}")

# Optionnel : colonnes de détail pour les tooltips d'arêtes
col_nb_semblables = None
col_nb_tot = None
for c in df.columns:
    cl = c.lower()
    if "nb_votes_semblables" in cl or "nb_semblables" in cl:
        col_nb_semblables = c
    if "nb_votes_tot" in cl or "nb_total" in cl or "nb_tot" in cl:
        col_nb_tot = c

# -----------------------------
# 3️⃣ Construction du graphe NetworkX
# -----------------------------
G = nx.Graph()

for _, r in df.iterrows():
    a1 = r["acteur1"]
    a2 = r["acteur2"]

    # Noeud 1
    G.add_node(
        a1,
        nom=r["depute1_nom"],
        prenom=r["depute1_prenom"],
        parti=r["parti1_libelleAbrev"],
    )

    # Noeud 2
    G.add_node(
        a2,
        nom=r["depute2_nom"],
        prenom=r["depute2_prenom"],
        parti=r["parti2_libelleAbrev"],
    )

    # Infos supplémentaires sur l'arête
    edge_data = {
        "ratio": float(r[ratio_col]),
    }
    if col_nb_semblables is not None:
        edge_data["nb_semblables"] = int(r[col_nb_semblables])
    if col_nb_tot is not None:
        edge_data["nb_tot"] = int(r[col_nb_tot])

    G.add_edge(a1, a2, **edge_data)

print(f"🕸 Nombre de nœuds : {G.number_of_nodes()}")
print(f"🕸 Nombre d'arêtes : {G.number_of_edges()}")

# -----------------------------
# 4️⃣ Palette de couleurs par parti
# -----------------------------
partis = sorted({d["parti"] for _, d in G.nodes(data=True)})

palette = [
    "#1f77b4",  # bleu
    "#ff7f0e",  # orange
    "#2ca02c",  # vert
    "#d62728",  # rouge
    "#9467bd",  # violet
    "#8c564b",  # marron
    "#e377c2",  # rose
    "#7f7f7f",  # gris
    "#17becf",  # cyan
    "#bcbd22",  # olive
]

parti_color = {p: palette[i % len(palette)] for i, p in enumerate(partis)}

# -----------------------------
# 5️⃣ Normalisation des largeurs d'arêtes
# -----------------------------
ratios = [d["ratio"] for _, _, d in G.edges(data=True)]
if ratios:
    rmin, rmax = min(ratios), max(ratios)
else:
    rmin = rmax = 1.0

def edge_width(r):
    if rmax != rmin:
        norm = (r - rmin) / (rmax - rmin)
    else:
        norm = 0.5
    # largeur entre 1 et 6
    return 1 + norm * 5

# -----------------------------
# 6️⃣ Création du graphe PyVis
# -----------------------------
net = Network(
    height="900px",
    width="100%",
    bgcolor="#ffffff",
    font_color="#222222",
    notebook=False,
    directed=False,
)

# Physique (layout interactif)
# ForceAtlas2-like
net.force_atlas_2based(
    gravity=-50,
    central_gravity=0.01,
    spring_length=200,
    spring_strength=0.01,
    damping=0.9,
)

# Ajout des nœuds
for node, data in G.nodes(data=True):
    nom = data["nom"]
    prenom = data["prenom"]
    parti = data["parti"]
    label = f"{nom} ({parti})"
    title = f"{prenom} {nom}<br>Parti : {parti}"

    net.add_node(
        node,
        label=label,
        title=title,
        color=parti_color.get(parti, "#cccccc"),
        shape="dot",
        size=12,
    )

# Ajout des arêtes
for u, v, data in G.edges(data=True):
    r = data["ratio"]
    width = edge_width(r)

    tooltip = f"Ratio votes semblables : {r:.1%}"
    nb_s = data.get("nb_semblables")
    nb_t = data.get("nb_tot")
    if nb_s is not None and nb_t is not None:
        tooltip += f"<br>{nb_s} votes semblables sur {nb_t}"

    net.add_edge(
        u,
        v,
        value=r,        # utilisé par l’algo de physique
        width=width,
        title=tooltip,
    )

# -----------------------------
# 7️⃣ Export HTML interactif
# -----------------------------
net.set_options("""
var options = {
  "nodes": {
    "borderWidth": 1,
    "borderWidthSelected": 2
  },
  "edges": {
    "smooth": false,
    "color": {
      "inherit": true
    }
  },
  "interaction": {
    "hover": true,
    "dragNodes": true,
    "selectConnectedEdges": true,
    "zoomView": true
  },
  "physics": {
    "stabilization": {
      "enabled": true,
      "iterations": 200
    }
  }
}
""")

net.write_html(str(OUT_HTML), open_browser=False)

print(f"✅ Graphe interactif généré : {OUT_HTML.resolve()}")
print("👉 Ouvre ce fichier dans ton navigateur pour explorer le graphe.")
