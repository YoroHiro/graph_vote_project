# votes_brut.py

import requests
import zipfile
from pathlib import Path
from time import sleep

# --------------------------
# Paramètres
# --------------------------
URL = "https://data.assemblee-nationale.fr/static/openData/repository/17/loi/scrutins/Scrutins.json.zip"

RAW_BASE_DIR = Path("../data_lake/raw/vote")
RAW_BASE_DIR.mkdir(parents=True, exist_ok=True)

# Fichier temporaire local
ZIP_TEMP = RAW_BASE_DIR / "scrutins_temp.zip"


# --------------------------
# Téléchargement robuste
# --------------------------
def telecharger_zip(url, dest_path, max_retries=5):
    for attempt in range(1, max_retries + 1):
        print(f"Téléchargement ZIP (tentative {attempt}/{max_retries})...")

        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()

                total = int(r.headers.get("Content-Length", 0))
                downloaded = 0

                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            # Progression (optionnel)
                            if total > 0:
                                pourcent = downloaded * 100 // total
                                print(f"\rProgression : {pourcent}% ({downloaded}/{total} bytes)", end="")
                print()  # saut de ligne
                
            print(f"✅ ZIP téléchargé : {dest_path}")
            return

        except Exception as e:
            print(f"\n❌ Erreur pendant le téléchargement : {e}")

            if attempt == max_retries:
                raise Exception("❌ Échec : Impossible de télécharger le ZIP même après retries.")
            else:
                print("⏳ Nouvelle tentative dans 5 secondes...")
                sleep(5)



# --------------------------
# 1️⃣ Téléchargement
# --------------------------
telecharger_zip(URL, ZIP_TEMP)


# --------------------------
# 2️⃣ Extraction
# --------------------------
print("📦 Extraction des JSON dans le dossier RAW...")

try:
    with zipfile.ZipFile(ZIP_TEMP, "r") as archive:
        fichiers = [f for f in archive.namelist() if f.startswith("json/") and f.endswith(".json")]

        print(f"➡️ {len(fichiers)} fichiers trouvés.")

        for fichier in fichiers:
            nom_simple = Path(fichier).name
            chemin_out = RAW_BASE_DIR / nom_simple

            with archive.open(fichier) as src, open(chemin_out, "wb") as dest:
                dest.write(src.read())

    print("🎉 Extraction terminée.")

except zipfile.BadZipFile:
    print("❌ Le ZIP téléchargé est corrompu.")
    print("💡 Supprime scrutins_temp.zip et relance pour réessayer.")

