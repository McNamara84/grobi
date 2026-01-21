import csv
import time
import json
import os

import requests

INPUT_FILE = "dois_unique.csv"
OUTPUT_DIR = "scholexplorer_responses"

BASE_URL = "https://api-beta.scholexplorer.openaire.eu/v3/Links"


def load_dois(path: str):
    dois = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doi = (row.get("DOI") or "").strip()
            if doi:
                dois.append(doi)
    return dois


def fetch_for_target_pid(doi: str):
    params = {"targetPid": doi}
    try:
        resp = requests.get(BASE_URL, params=params, timeout=30)
    except requests.RequestException as e:
        print(f"Fehler bei DOI {doi}: {e}")
        return None, "error"

    if resp.status_code == 200:
        try:
            return resp.json(), "ok"
        except ValueError:
            print(f"Keine gültige JSON-Antwort für {doi}")
            return None, "error"
    else:
        print(f"HTTP {resp.status_code} für DOI {doi}")
        return None, "error"


def safe_filename_from_doi(doi: str) -> str:
    return doi.replace("/", "_").replace(":", "_")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    dois = load_dois(INPUT_FILE)
    print(f"{len(dois)} DOIs geladen.")

    for i, doi in enumerate(dois, start=1):
        print(f"[{i}/{len(dois)}] Hole ScholeXplorer-Daten für {doi} ...")
        data, status = fetch_for_target_pid(doi)
        fname = safe_filename_from_doi(doi) + ".json"
        out_path = os.path.join(OUTPUT_DIR, fname)

        if status == "ok" and data is not None:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"  -> gespeichert in {out_path}")
        else:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({"error": True}, f)
            print(f"  -> Fehler, Platzhalter-JSON in {out_path}")

        time.sleep(0.3)

    print("Fertig.")


if __name__ == "__main__":
    main()
