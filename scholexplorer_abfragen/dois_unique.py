input_file = "dois.csv"
output_file = "dois_unique.csv"

seen = set()
with open(input_file, encoding="utf-8") as fin, open(output_file, "w", encoding="utf-8") as fout:
    header = fin.readline()
    fout.write(header) 
    for line in fin:
        doi = line.strip()
        if doi and doi not in seen:
            seen.add(doi)
            fout.write(line)
