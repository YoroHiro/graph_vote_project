# organe_brut.py

import requests
import zipfile
import io
from pathlib import Path
from time import sleep

# --------------------------
# Paramètres
# --------------------------
url = "https://data.assemblee-nationale.fr/static/openData/repository/17/amo/deputes_actifs_mandats_actifs_organes/AMO10_deputes_actifs_mandats_actifs_organes.json.zip"
RAW_BASE_DIR = Path("../data_lake/raw/organe")
RAW_BASE_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------
# Téléchargement du ZIP (avec retries)
# --------------------------
def telecharger_zip(url, max_retries=3, timeout=60):
    for attempt in range(1, max_retries + 1):
        print(f"Téléchargement du fichier ZIP... (tentative {attempt}/{max_retries})")
        try:
            # stream=True pour lire par morceaux
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                buffer = io.BytesIO()
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:  # filtrer les keep-alive
                        buffer.write(chunk)
                buffer.seek(0)
                return buffer
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur réseau : {e}")
            if attempt == max_retries:
                print("❌ Échec après plusieurs tentatives, abandon.")
                raise
            else:
                print("⏳ On réessaie dans 5 secondes...")
                sleep(5)

# --------------------------
# Extraction du ZIP
# --------------------------
zip_buffer = telecharger_zip(url)

print("📦 Vérification et extraction du ZIP en mémoire...")
try:
    with zipfile.ZipFile(zip_buffer) as archive:
        fichiers_organe = [
            f for f in archive.namelist()
            if f.startswith("json/organe/") and f.endswith(".json")
        ]

        print(f"✅ {len(fichiers_organe)} fichiers trouvés dans 'json/organe/'")

        for nom_fichier in fichiers_organe:
            nom_simple = Path(nom_fichier).name
            chemin_local = RAW_BASE_DIR / nom_simple

            with archive.open(nom_fichier) as source, open(chemin_local, "wb") as cible:
                cible.write(source.read())

    print(f"🎉 Extraction terminée — fichiers enregistrés dans : {RAW_BASE_DIR.resolve()}")

except zipfile.BadZipFile:
    print("❌ Le fichier téléchargé n'est pas un ZIP valide (téléchargement incomplet ?)")
