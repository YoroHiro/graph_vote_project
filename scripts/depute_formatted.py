import json
import csv
from pathlib import Path

# -----------------------------
# 1️⃣ Chemins
# -----------------------------
DOSSIER_RAW = Path("../data_lake/raw/depute")
FICHIER_SORTIE = Path("../data_lake/formatted/depute/depute_formatted.csv")
FICHIER_SORTIE.parent.mkdir(parents=True, exist_ok=True)

# -----------------------------
# 2️⃣ Colonnes du CSV de sortie
# -----------------------------
colonnes = [
    "uid",
    "prenom",
    "nom",
    "profession",
    "catSocPro",
    "uri_hatvp",
    "id_gp",
    "id_par_pol",
    "nb_mandats"
]

print("🔍 Extraction des députés JSON → CSV...")

# -----------------------------
# 3️⃣ Écriture du CSV
# -----------------------------
with open(FICHIER_SORTIE, "w", newline="", encoding="utf-8") as f_csv:
    writer = csv.DictWriter(f_csv, fieldnames=colonnes)
    writer.writeheader()

    for fichier in DOSSIER_RAW.glob("*.json"):
        try:
            with open(fichier, "r", encoding="utf-8") as f:
                data = json.load(f)

            acteur = data.get("acteur", {})

            # -----------------------------
            # UID
            # -----------------------------
            uid = acteur.get("uid", {}).get("#text")

            # -----------------------------
            # État civil
            # -----------------------------
            etatCivil = acteur.get("etatCivil", {})
            ident = etatCivil.get("ident", {})

            prenom = ident.get("prenom")
            nom = ident.get("nom")

            # -----------------------------
            # Profession
            # -----------------------------
            profession = acteur.get("profession", {}).get("libelleCourant")
            catSocPro = acteur.get("profession", {}).get("socProcINSEE", {}).get("catSocPro")

            # -----------------------------
            # URI HATVP
            # -----------------------------
            uri_hatvp = acteur.get("uri_hatvp")

            # -----------------------------
            # Mandats : on récupère GP & PARPOL
            # -----------------------------
            id_gp = None
            id_par_pol = None
            nb_mandats = 0

            mandats = acteur.get("mandats", {}).get("mandat", [])
            if isinstance(mandats, dict):
                mandats = [mandats]

            for m in mandats:
                nb_mandats += 1
                type_org = m.get("typeOrgane")
                org_ref = m.get("organes", {}).get("organeRef")

                # Normalisation organeRef potentiel tableau
                if isinstance(org_ref, list):
                    org_refs = org_ref
                else:
                    org_refs = [org_ref]

                # Groupe parlementaire
                if type_org == "GP":
                    for o in org_refs:
                        if o and o.startswith("PO"):
                            id_gp = o

                # Parti politique
                if type_org == "PARPOL":
                    for o in org_refs:
                        if o and o.startswith("PO"):
                            id_par_pol = o

            # -----------------------------
            # Ligne finale
            # -----------------------------
            writer.writerow({
                "uid": uid,
                "prenom": prenom,
                "nom": nom,
                "profession": profession,
                "catSocPro": catSocPro,
                "uri_hatvp": uri_hatvp,
                "id_gp": id_gp,
                "id_par_pol": id_par_pol,
                "nb_mandats": nb_mandats
            })

            print(f"✅ {fichier.name} traité.")

        except Exception as e:
            print(f"⚠️ Erreur avec {fichier.name} : {e}")

print(f"\n📦 Extraction terminée → {FICHIER_SORTIE}")
