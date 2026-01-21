# Remove duplicate DOIs from input CSV while preserving order (unique filtering)
input_file = "dois.csv"
output_file = "dois_unique.csv"

seen = set() # Tracks DOIs already encountered (for uniqueness check)

# Read input CSV, keep first occurrence of each DOI, write result to output CSV
with open(input_file, encoding="utf-8") as fin, open(output_file, "w", encoding="utf-8") as fout:
    header = fin.readline()
    fout.write(header) 
    for line in fin:
        doi = line.strip()
        if doi and doi not in seen:
            seen.add(doi)
            fout.write(line)
