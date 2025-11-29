import json, csv
from pathlib import Path

RAW_DIR = Path("../data_lake/raw/depute")
CSV_OUT = Path("../data_lake/usage/deputes_partis.csv")

with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["acteurRef", "partiRef"])
    writer.writeheader()
    for fichier in RAW_DIR.glob("*.json"):
        data = json.load(open(fichier))
        acteur = data["acteur"]
        acteurRef = acteur["uid"]["#text"]
        partiRef = None
        for mandat in acteur["mandats"]["mandat"]:
            if mandat["typeOrgane"] == "GP":  # Groupe parlementaire
                partiRef = mandat["organes"]["organeRef"]
                break
        if partiRef:
            writer.writerow({"acteurRef": acteurRef, "partiRef": partiRef})
