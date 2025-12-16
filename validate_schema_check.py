"""
Validierungsskript für Schema-Check Ergebnisse
Prüft alle 9 DOIs aus der CSV direkt gegen die DataCite API
"""
import requests
from requests.auth import HTTPBasicAuth

# Credentials
USERNAME = "TIB.GFZ"
PASSWORD = "Coosar3k"
BASE_URL = "https://api.datacite.org"

# Alle 9 DOIs aus der CSV-Datei
csv_dois = [
    "10.1594/gfz.geofon.gfz2008jhne",
    "10.1594/gfz.geofon.gfz2011ewla",
    "10.1594/gfz.isdc.champ/ch-og-3-rso",
    "10.1594/gfz/icdp/con/2004",
    "10.1594/gfz/isdc/champ/ch-ai-3-atm-20040501-20040531",
    "10.1594/gfz/isdc/champ/ch-me-4-mod-gm+co2-2002-01-10",
    "10.1594/gfz/isdc/champ/ch-og-4-egm+m-00-07-30-01-12-31-001.1",
    "10.5880/gfz.2.1.2017.002",
    "10.14470/wy6x-1785",
]

# Gültige ContributorTypes nach DataCite Schema 4.x
VALID_CONTRIBUTOR_TYPES = [
    "ContactPerson", "DataCollector", "DataCurator", "DataManager",
    "Distributor", "Editor", "HostingInstitution", "Producer",
    "ProjectLeader", "ProjectManager", "ProjectMember", "RegistrationAgency",
    "RegistrationAuthority", "RelatedPerson", "Researcher",
    "ResearchGroup", "RightsHolder", "Sponsor", "Supervisor",
    "WorkPackageLeader", "Other"
]

auth = HTTPBasicAuth(USERNAME, PASSWORD)

def check_doi(doi):
    """Prüft einen DOI und gibt die relevanten Metadaten aus"""
    encoded_doi = doi.replace("/", "%2F")
    url = f"{BASE_URL}/dois/{encoded_doi}"
    
    try:
        response = requests.get(url, auth=auth, timeout=30)
        if response.status_code == 200:
            data = response.json()
            attrs = data.get("data", {}).get("attributes", {})
            
            schema_version = attrs.get("schemaVersion", "NICHT VORHANDEN")
            publisher = attrs.get("publisher")
            pub_year = attrs.get("publicationYear")
            titles = attrs.get("titles", [])
            creators = attrs.get("creators", [])
            types = attrs.get("types", {})
            resource_type = types.get("resourceTypeGeneral") if types else None
            contributors = attrs.get("contributors", [])
            state = attrs.get("state")
            
            print(f"\n{'='*70}")
            print(f"DOI: {doi}")
            print(f"{'='*70}")
            print(f"Schema Version: {schema_version}")
            print(f"State: {state}")
            print()
            
            # Pflichtfelder prüfen
            problems = []
            
            # Publisher
            if not publisher:
                print(f"  ❌ Publisher: FEHLT")
                problems.append("Publisher fehlt")
            elif isinstance(publisher, dict):
                pub_name = publisher.get("name", "")
                if not pub_name or not pub_name.strip():
                    print(f"  ❌ Publisher: LEER (Objekt ohne Namen)")
                    problems.append("Publisher fehlt")
                else:
                    print(f"  ✓ Publisher: {pub_name}")
            elif isinstance(publisher, str) and not publisher.strip():
                print(f"  ❌ Publisher: LEER")
                problems.append("Publisher fehlt")
            else:
                print(f"  ✓ Publisher: {publisher}")
            
            # Publication Year
            if not pub_year:
                print(f"  ❌ Publication Year: FEHLT")
                problems.append("Erscheinungsjahr fehlt")
            else:
                print(f"  ✓ Publication Year: {pub_year}")
            
            # Titles
            if not titles or len(titles) == 0:
                print(f"  ❌ Titles: LEER/FEHLT")
                problems.append("Titel fehlt")
            else:
                # Prüfe ob mindestens ein nicht-leerer Titel existiert
                non_empty = [t for t in titles if t.get("title", "").strip()]
                if not non_empty:
                    print(f"  ❌ Titles: Alle Titel sind leer")
                    problems.append("Titel fehlt")
                else:
                    print(f"  ✓ Titles: {len(non_empty)} Titel vorhanden")
                    for t in non_empty[:2]:
                        title_text = t.get('title', '?')
                        if len(title_text) > 60:
                            title_text = title_text[:60] + "..."
                        print(f"      - {title_text}")
            
            # Creators
            if not creators or len(creators) == 0:
                print(f"  ❌ Creators: LEER/FEHLT")
                problems.append("Creators fehlen")
            else:
                print(f"  ✓ Creators: {len(creators)} Creator(s)")
                for c in creators[:3]:
                    print(f"      - {c.get('name', '?')}")
            
            # Resource Type
            if not resource_type:
                print(f"  ❌ Resource Type General: FEHLT")
                problems.append("ResourceType fehlt")
            else:
                print(f"  ✓ Resource Type General: {resource_type}")
            
            # Contributors prüfen (für Funder-Problem)
            if contributors:
                contrib_types = [c.get("contributorType") for c in contributors]
                unknown_types = [t for t in contrib_types if t and t not in VALID_CONTRIBUTOR_TYPES]
                if unknown_types:
                    print(f"  ⚠ Unbekannte Contributor Types: {unknown_types}")
                    problems.append(f"Unbekannte ContributorTypes: {unknown_types}")
                else:
                    print(f"  ✓ Contributors: {len(contributors)} (alle gültige Types)")
            
            print()
            if problems:
                print(f"  📋 PROBLEME BESTÄTIGT: {'; '.join(problems)}")
                return True
            else:
                print(f"  ⚠ KEINE PROBLEME GEFUNDEN - Sollte NICHT in CSV sein!")
                return False
            
        elif response.status_code == 404:
            print(f"\n{'='*70}")
            print(f"DOI: {doi}")
            print(f"{'='*70}")
            print(f"  ❌ DOI NICHT GEFUNDEN (HTTP 404)")
            return None
        else:
            print(f"\nDOI {doi}: HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"\nDOI {doi}: Error - {e}")
        return None


print("="*70)
print("VALIDIERUNG DER 9 DOIs AUS DER SCHEMA-CHECK CSV")
print("="*70)
print(f"Prüfe gegen: {BASE_URL}")
print(f"Account: {USERNAME}")

validated_count = 0
false_positives = 0
errors = 0

for doi in csv_dois:
    result = check_doi(doi)
    if result is True:
        validated_count += 1
    elif result is False:
        false_positives += 1
    else:
        errors += 1

print("\n\n" + "="*70)
print("ZUSAMMENFASSUNG")
print("="*70)
print(f"DOIs in CSV:                    {len(csv_dois)}")
print(f"Probleme bestätigt:             {validated_count} ✓")
print(f"Falsch-Positive (kein Problem): {false_positives}")
print(f"Fehler bei Abfrage:             {errors}")
print()

if validated_count == len(csv_dois):
    print("✅ VALIDIERUNG ERFOLGREICH - Alle DOIs haben bestätigte Probleme!")
elif false_positives > 0:
    print(f"⚠ ACHTUNG: {false_positives} DOI(s) haben keine erkennbaren Probleme!")
else:
    print("✅ Validierung abgeschlossen")

# Gesamtanzahl DOIs prüfen
print("\n" + "="*70)
print("GESAMTANZAHL DOIs für TIB.GFZ")
print("="*70)
url = f"{BASE_URL}/dois?client-id={USERNAME}&page[size]=1"
response = requests.get(url, auth=auth, timeout=30)
if response.status_code == 200:
    total = response.json().get("meta", {}).get("total", 0)
    print(f"Total DOIs im Account:  {total}")
    print(f"DOIs mit Problemen:     {len(csv_dois)} ({len(csv_dois)/total*100:.3f}%)")
    print(f"DOIs ohne Probleme:     {total - len(csv_dois)} ({(total-len(csv_dois))/total*100:.2f}%)")
