# organes_formatted.py

import json
import pandas as pd
from pathlib import Path

RAW_DIR = Path("../data_lake/raw/organe")
OUT_FILE = Path("../data_lake/formatted/organes/organes_formatted.csv")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

rows = []

print("🧩 Extraction des organes GP & PARPOL...")

for fichier in RAW_DIR.glob("*.json"):
    if "Zone.Identifier" in fichier.name:
        continue

    with open(fichier, "r", encoding="utf-8") as f:
        data = json.load(f)

    org = data.get("organe", {})
    codeType = org.get("codeType")

    # On garde seulement GP & PARPOL
    if codeType not in ["GP", "PARPOL"]:
        continue

    rows.append({
        "uid": org.get("uid"),
        "codeType": codeType,
        "libelle": org.get("libelle"),
        "libelleAbrege": org.get("libelleAbrege"),
        "libelleAbrev": org.get("libelleAbrev"),
        "regime": org.get("regime"),
        "legislature": org.get("legislature"),
        "region": org.get("lieu", {}).get("region", {}).get("libelle") if org.get("lieu") else None,
        "departement": org.get("lieu", {}).get("departement", {}).get("libelle") if org.get("lieu") else None
    })

df = pd.DataFrame(rows)
df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
print(f"✅ {len(df)} organes GP/PARPOL sauvegardés dans : {OUT_FILE}")
