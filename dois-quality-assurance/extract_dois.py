import csv
from pathlib import Path

BASE_DIR = Path(__file__).parent
INPUT_FILE = BASE_DIR / "TIB.GFZ_urls.csv"
OUTPUT_FILE = BASE_DIR / "gfz_dois_only.csv"

def main():
    with open(INPUT_FILE, newline="", encoding="utf-8") as infile, \
         open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["DOI"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            doi = row.get("DOI")
            if doi:
                writer.writerow({"DOI": doi.strip()})

if __name__ == "__main__":
    main()
