import requests
import zipfile
import io
from pathlib import Path
from datetime import datetime

# --------------------------
# Paramètres
# --------------------------
url = "https://data.assemblee-nationale.fr/static/openData/repository/17/amo/deputes_actifs_mandats_actifs_organes/AMO10_deputes_actifs_mandats_actifs_organes.json.zip"
RAW_BASE_DIR = Path("../data_lake/raw/depute")
RAW_BASE_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------
# Téléchargement du ZIP
# --------------------------
print("Téléchargement du fichier ZIP...")
try:
    response = requests.get(url)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"Erreur réseau : {e}")
    exit()

# --------------------------
# Extraction du ZIP en mémoire
# --------------------------
print("Extraction du contenu en mémoire...")
with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
    fichiers_acteur = [f for f in archive.namelist() if f.startswith("json/acteur/") and f.endswith(".json")]

    print(f"{len(fichiers_acteur)} fichiers trouvés dans 'json/acteur/'")

    # --------------------------
    # Sauvegarde locale des fichiers acteur
    # --------------------------
    for nom_fichier in fichiers_acteur:
        nom_simple = Path(nom_fichier).name
        chemin_local = RAW_BASE_DIR / nom_simple

        with archive.open(nom_fichier) as source, open(chemin_local, "wb") as cible:
            cible.write(source.read())

print(f"Extraction terminée — fichiers enregistrés dans : {RAW_BASE_DIR.resolve()}")
