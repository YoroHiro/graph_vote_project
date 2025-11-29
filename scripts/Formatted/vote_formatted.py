import json
import csv
from pathlib import Path

# --- Configuration des chemins ---
DOSSIER_RAW = Path("../data_lake/raw/vote")
FICHIER_SORTIE = Path("../data_lake/formatted/votes_formatted.csv")
FICHIER_SORTIE.parent.mkdir(parents=True, exist_ok=True)

# --- Colonnes du CSV ---
colonnes = [
    "uid_scrutin", "dateScrutin", "titre",
    "organeRef_groupe", "acteurRef", "mandatRef",
    "vote_position", "parDelegation", "numPlace"
]

print("🔍 Extraction des votes JSON → CSV...")

with open(FICHIER_SORTIE, "w", newline="", encoding="utf-8") as f_csv:
    writer = csv.DictWriter(f_csv, fieldnames=colonnes)
    writer.writeheader()

    for fichier in DOSSIER_RAW.glob("*.json"):
        try:
            with open(fichier, "r", encoding="utf-8") as f:
                data = json.load(f)
                scrutin = data.get("scrutin", {})

                uid = scrutin.get("uid")
                dateScrutin = scrutin.get("dateScrutin")
                titre = scrutin.get("titre", "").strip()

                groupes = (
                    scrutin
                    .get("ventilationVotes", {})
                    .get("organe", {})
                    .get("groupes", {})
                    .get("groupe", [])
                )

                if isinstance(groupes, dict):
                    groupes = [groupes]

                for g in groupes:
                    organeRef_groupe = g.get("organeRef")
                    vote = g.get("vote", {})
                    decompte = vote.get("decompteNominatif", {})

                    for position in ["pours", "contres", "abstentions", "nonVotants"]:
                        bloc = decompte.get(position)
                        if not bloc:
                            continue

                        votants = bloc.get("votant")
                        if not votants:
                            continue
                        if isinstance(votants, dict):
                            votants = [votants]

                        for v in votants:
                            writer.writerow({
                                "uid_scrutin": uid,
                                "dateScrutin": dateScrutin,
                                "titre": titre,
                                "organeRef_groupe": organeRef_groupe,
                                "acteurRef": v.get("acteurRef"),
                                "mandatRef": v.get("mandatRef"),
                                "vote_position": position[:-1] if position.endswith("s") else position,
                                "parDelegation": v.get("parDelegation"),
                                "numPlace": v.get("numPlace")
                            })

            print(f"✅ {fichier.name} traité.")
        except Exception as e:
            print(f"⚠️ Erreur avec {fichier.name} : {e}")

print(f"\n✅ Extraction terminée → {FICHIER_SORTIE}")
