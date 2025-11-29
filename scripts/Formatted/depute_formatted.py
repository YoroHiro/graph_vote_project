import json
import csv
from pathlib import Path

RAW_BASE_DIR = Path("../data_lake/raw/depute")
CSV_FILE = Path("../data_lake/formatted/depute_formatted.csv")
CSV_FILE.parent.mkdir(exist_ok=True)

# Colonnes du CSV
colonnes = [
    "uid", "prenom", "nom", "profession", "catSocPro", "uri_hatvp",
    "id_gp", "id_par_pol", "nb_mandats"
]

with open(CSV_FILE, "w", newline="", encoding="utf-8") as f_csv:
    writer = csv.DictWriter(f_csv, fieldnames=colonnes)
    writer.writeheader()

    # Parcours des fichiers JSON
    for fichier in RAW_BASE_DIR.glob("*.json"):
        with open(fichier, "r", encoding="utf-8") as f:
            data = json.load(f)

        acteur = data.get("acteur", {})
        uid = acteur.get("uid", {}).get("#text", "Inconnu")

        etat_civil = acteur.get("etatCivil", {})
        ident = etat_civil.get("ident", {})
        prenom = ident.get("prenom", "Inconnu")
        nom = ident.get("nom", "Inconnu")

        profession = acteur.get("profession", {}).get("libelleCourant", "Inconnu")
        catSocPro = acteur.get("profession", {}).get("socProcINSEE", {}).get("catSocPro", "Non renseignée")
        uri_hatvp = acteur.get("uri_hatvp", "")

        # ---- Mandats et organes ----
        mandats = acteur.get("mandats", {}).get("mandat", [])
        if isinstance(mandats, dict):
            mandats = [mandats]
        nb_mandats = len(mandats)

        # ---- Extraction du groupe parlementaire (GP) et parti politique (PARPOL) ----
        id_gp = "Inconnu"
        id_par_pol = "Inconnu"

        for mandat in mandats:
            type_organe = mandat.get("typeOrgane")
            organes = mandat.get("organes", {})

            if type_organe == "GP":  # groupe parlementaire
                id_gp = organes.get("organeRef", id_gp)

            if type_organe == "PARPOL":  # parti politique
                id_par_pol = organes.get("organeRef", id_par_pol)

        # ---- Écriture dans le CSV ----
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

print(f"CSV généré dans {CSV_FILE.resolve()}")
