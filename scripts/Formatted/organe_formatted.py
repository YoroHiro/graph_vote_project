import json
import pandas as pd
from pathlib import Path

# --- Chemins ---
RAW_DIR = Path("../data_lake/raw/organe")
CSV_FILE = Path("../data_lake/formatted/organes_formatted.csv")
CSV_FILE.parent.mkdir(parents=True, exist_ok=True)

# --- Initialisation ---
rows = []

print("🧩 Extraction des organes politiques (GP & PARPOL) depuis les fichiers JSON...")

# --- Parcours des fichiers JSON ---
for fichier in RAW_DIR.glob("*.json"):
    try:
        with open(fichier, "r", encoding="utf-8") as f:
            data = json.load(f)

        organe = data.get("organe", {})
        codeType = organe.get("codeType")

        # --- Filtrage : on garde uniquement GP et PARPOL ---
        if codeType not in ["GP", "PARPOL"]:
            continue

        uid = organe.get("uid")
        libelle = organe.get("libelle")
        libelleAbrege = organe.get("libelleAbrege")
        libelleAbrev = organe.get("libelleAbrev")
        regime = organe.get("regime")
        legislature = organe.get("legislature")

        # Informations géographiques (souvent absentes pour GP/PARPOL)
        region = (
            organe.get("lieu", {})
            .get("region", {})
            .get("libelle")
            if organe.get("lieu") else None
        )
        departement = (
            organe.get("lieu", {})
            .get("departement", {})
            .get("libelle")
            if organe.get("lieu") else None
        )

        rows.append({
            "uid": uid,
            "codeType": codeType,
            "libelle": libelle,
            "libelleAbrege": libelleAbrege,
            "libelleAbrev": libelleAbrev,
            "regime": regime,
            "legislature": legislature,
            "region": region,
            "departement": departement
        })

    except Exception as e:
        print(f"⚠️ Erreur lors du traitement de {fichier.name}: {e}")

# --- Export final ---
df = pd.DataFrame(rows)

if df.empty:
    print("⚠️ Aucun organe trouvé avec codeType = GP ou PARPOL")
else:
    df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df)} organes GP/PARPOL sauvegardés dans : {CSV_FILE}")
