# depute_formatted.py

import json
import pandas as pd
from pathlib import Path

RAW_DIR = Path("../data_lake/raw/depute")
OUT_FILE = Path("../data_lake/formatted/depute/depute_formatted.csv")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

rows = []

print("🧩 Extraction des députés...")

for fichier in RAW_DIR.glob("*.json"):
    if "Zone.Identifier" in fichier.name:
        continue  # Ignore fichiers parasites Windows

    with open(fichier, "r", encoding="utf-8") as f:
        data = json.load(f)

    acteur = data.get("acteur", {})
    etatCivil = acteur.get("etatCivil", {})
    ident = etatCivil.get("ident", {})
    profession = acteur.get("profession", {})

    # Identité
    uid = acteur.get("uid")
    prenom = ident.get("prenom")
    nom = ident.get("nom")
    profession_libelle = profession.get("libelleCourant")
    catSocPro = acteur.get("catSocPro")
    uri_hatvp = acteur.get("uri_hatvp")

    # Nombre de mandats
    mandats = acteur.get("mandats", {}).get("mandat", [])
    nb_mandats = len(mandats) if isinstance(mandats, list) else 1

    # Groupe politique (id_gp)
    id_gp = None
    for m in mandats if isinstance(mandats, list) else [mandats]:
        if m.get("typeOrgane") == "GP":
            id_gp = m.get("organes", {}).get("organeRef")

    # Parti politique (id_par_pol)
    id_par_pol = None
    for m in mandats if isinstance(mandats, list) else [mandats]:
        if m.get("typeOrgane") == "PARPOL":
            id_par_pol = m.get("organes", {}).get("organeRef")

    rows.append({
        "uid": uid,
        "prenom": prenom,
        "nom": nom,
        "profession": profession_libelle,
        "catSocPro": catSocPro,
        "uri_hatvp": uri_hatvp,
        "id_gp": id_gp,
        "id_par_pol": id_par_pol,
        "nb_mandats": nb_mandats
    })

df = pd.DataFrame(rows)
df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
print(f"✅ {len(df)} députés sauvegardés dans : {OUT_FILE}")
