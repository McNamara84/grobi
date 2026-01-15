import json
import xml.etree.ElementTree as ET
import re
import sys
from pathlib import Path


def normalize_doi(raw):
    """
    Normalize DOI for comparison:
    - strip spaces
    - remove leading https://doi.org/ etc.
    - lower-case
    """
    if not raw:
        return None
    doi = raw.strip()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.IGNORECASE)
    return doi.lower()


def extract_dois_from_xml(xml_path):
    """
    Extract ALL DOIs from the landingpage XML where:
      relatedIdentifierType="DOI"
    (relationType ist egal: IsCitedBy, HasPart, IsPartOf, etc.)
    Returns a set of normalized DOIs.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Namespace aus Root-Tag holen, z.B. {http://datacite.org/schema/kernel-4}
    ns = {"d": root.tag.split("}")[0].strip("{")}

    dois = set()

    for rel in root.findall(".//d:relatedIdentifier", ns):
        rel_id_type = rel.attrib.get("relatedIdentifierType", "").lower()

        if rel_id_type == "doi":
            norm = normalize_doi(rel.text)
            if norm:
                dois.add(norm)

    return dois


def extract_dois_from_json(json_path):
    """
    Extract ALL DOIs from ScholExplorer JSON:
    - aus source.Identifier[...].ID, wenn IDScheme="doi"
    - aus target.Identifier[...].ID, wenn IDScheme="doi"
    RelationshipType (cites, isRelatedTo, ...) ist egal.
    Returns a set of normalized DOIs.
    """
    with open(json_path, encoding="utf-8") as f:
        obj = json.load(f)

    dois = set()

    for link in obj.get("result", []):
        # both source and target sides may contain DOIs
        for side in ("source", "target"):
            entity = link.get(side, {})
            for ident in entity.get("Identifier", []):
                if ident.get("IDScheme", "").lower() == "doi":
                    norm = normalize_doi(ident.get("ID"))
                    if norm:
                        dois.add(norm)

    return dois


def compare_one(xml_path, json_path):
    """
    Compare DOIs from XML landingpage and ScholExplorer JSON
    and print a small report.
    """
    xml_path = Path(xml_path)
    json_path = Path(json_path)

    print(f"=== Vergleich für\n  XML : {xml_path}\n  JSON: {json_path}\n")

    xml_dois = extract_dois_from_xml(xml_path)
    json_dois = extract_dois_from_json(json_path)

    json_not_in_xml = sorted(json_dois - xml_dois)
    xml_not_in_json = sorted(xml_dois - json_dois)
    in_both = sorted(json_dois & xml_dois)

    print(f"Anzahl DOIs in XML (alle relatedIdentifier mit DOI): {len(xml_dois)}")
    print(f"Anzahl DOIs in JSON (alle source/target DOIs): {len(json_dois)}\n")

    print(f"DOIs NUR in JSON (fehlen in XML): {len(json_not_in_xml)}")
    for doi in json_not_in_xml:
        print(f"  + {doi}")
    print()

    print(f"DOIs NUR in XML (nicht in JSON): {len(xml_not_in_json)}")
    for doi in xml_not_in_json:
        print(f"  - {doi}")
    print()

    print(f"DOIs in BEIDEN Quellen: {len(in_both)}")
    for doi in in_both:
        print(f"  = {doi}")
    print()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Verwendung:")
        print("  python compare_citations.py path/to/landingpage.xml path/to/scholexplorer.json")
        sys.exit(1)

    xml_file = sys.argv[1]
    json_file = sys.argv[2]

    compare_one(xml_file, json_file)
