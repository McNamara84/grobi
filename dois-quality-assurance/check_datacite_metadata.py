import csv
import time
import urllib.parse

import requests

# INPUT_FILE = "gfz_dois_only.csv"
# OUTPUT_FILE = "gfz_doi_metadata_check.csv"
INPUT_FILE = "test.csv"
OUTPUT_FILE = "testergebnisse.csv"
DATACITE_API_BASE = "https://api.datacite.org/dois/"

def fetch_datacite_metadata(doi: str):
    url = DATACITE_API_BASE + urllib.parse.quote(doi)
    try:
        resp = requests.get(url, timeout=10)
    except requests.RequestException:
        return None, "invalid"

    if resp.status_code == 200:
        return resp.json(), "valid"
    else:
        return None, "invalid"

def check_fields(doi: str, meta, status: str):
    if status != "valid" or meta is None:
        return {
            "DOI": doi,
            "status": "invalid",
            "has_title": "nicht vorhanden",
            "has_publisher": "nicht vorhanden",
            "has_year": "nicht vorhanden",
            "has_resource_type": "nicht vorhanden",
            "has_creator": "nicht vorhanden",
        }

    attrs = (meta.get("data") or {}).get("attributes") or {}

    # Titel
    titles = attrs.get("titles") or []
    has_title = "vorhanden" if any(t.get("title") for t in titles) else "nicht vorhanden"

    # Publisher
    publisher = attrs.get("publisher")
    has_publisher = "vorhanden" if publisher else "nicht vorhanden"

    # Jahr
    year = attrs.get("publicationYear")
    has_year = "vorhanden" if year else "nicht vorhanden"

    # Ressourcentyp
    resource_type = (attrs.get("types") or {}).get("resourceTypeGeneral")
    has_resource_type = "vorhanden" if resource_type else "nicht vorhanden"

    # Creator
    creators = attrs.get("creators") or []
    has_creator = "vorhanden" if any(c.get("name") for c in creators) else "nicht vorhanden"

    return {
        "DOI": doi,
        "status": "valid",
        "has_title": has_title,
        "has_publisher": has_publisher,
        "has_year": has_year,
        "has_resource_type": has_resource_type,
        "has_creator": has_creator,
    }

def load_dois(path: str):
    dois = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doi = (row.get("DOI") or "").strip()
            if doi:
                dois.append(doi)
    return dois

def main():
    dois = load_dois(INPUT_FILE)

    results = []
    for i, doi in enumerate(dois, start=1):
        print(f"[{i}/{len(dois)}] Checking {doi}...")
        meta, status = fetch_datacite_metadata(doi)
        result = check_fields(doi, meta, status)
        results.append(result)
        time.sleep(0.2)

    fieldnames = [
        "DOI",
        "status",
        "has_title",
        "has_publisher",
        "has_year",
        "has_resource_type",
        "has_creator",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"Fertig. Ergebnis in {OUTPUT_FILE}")

if __name__ == "__main__":
    main()