"""
Vergleiche DataCite API-Daten mit Datenbank-Einträgen
um zu verstehen, woher die zusätzlichen resourceagent-Einträge kommen
"""

import requests
import json

def fetch_from_datacite(doi):
    """Hole Metadaten von DataCite API"""
    url = f"https://api.datacite.org/dois/{doi}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"[X] Fehler beim Abruf von DataCite: {response.status_code}")
        return None
    
    return response.json()

def analyze_datacite_metadata():
    """Analysiere DataCite Metadaten für Test-DOI"""
    test_doi = "10.5880/gfz_orbit/rso/gnss_g_v02"
    
    print("=" * 80)
    print(f"DATACITE METADATEN: {test_doi}")
    print("=" * 80)
    
    data = fetch_from_datacite(test_doi)
    if not data:
        return
    
    attributes = data.get('data', {}).get('attributes', {})
    
    # Creators (Autoren)
    creators = attributes.get('creators', [])
    print(f"\nCREATORS (Autoren): {len(creators)}")
    print("-" * 80)
    for i, creator in enumerate(creators, 1):
        print(f"\nCreator #{i}:")
        print(f"  Name: {creator.get('name', 'N/A')}")
        print(f"  Given Name: {creator.get('givenName', 'N/A')}")
        print(f"  Family Name: {creator.get('familyName', 'N/A')}")
        print(f"  Name Type: {creator.get('nameType', 'N/A')}")
        
        identifiers = creator.get('nameIdentifiers', [])
        if identifiers:
            for ident in identifiers:
                print(f"  Identifier: {ident.get('nameIdentifier', 'N/A')} ({ident.get('nameIdentifierScheme', 'N/A')})")
        
        affiliations = creator.get('affiliation', [])
        if affiliations:
            affil_names = [a.get('name', a) if isinstance(a, dict) else a for a in affiliations]
            print(f"  Affiliations: {', '.join(affil_names)}")
    
    # Contributors
    contributors = attributes.get('contributors', [])
    print(f"\n\nCONTRIBUTORS: {len(contributors)}")
    print("-" * 80)
    for i, contributor in enumerate(contributors, 1):
        print(f"\nContributor #{i}:")
        print(f"  Name: {contributor.get('name', 'N/A')}")
        print(f"  Given Name: {contributor.get('givenName', 'N/A')}")
        print(f"  Family Name: {contributor.get('familyName', 'N/A')}")
        print(f"  Contributor Type: {contributor.get('contributorType', 'N/A')}")
        print(f"  Name Type: {contributor.get('nameType', 'N/A')}")
        
        identifiers = contributor.get('nameIdentifiers', [])
        if identifiers:
            for ident in identifiers:
                print(f"  Identifier: {ident.get('nameIdentifier', 'N/A')} ({ident.get('nameIdentifierScheme', 'N/A')})")
    
    # Zusammenfassung
    print("\n" + "=" * 80)
    print("ZUSAMMENFASSUNG")
    print("=" * 80)
    print(f"DataCite Creators: {len(creators)}")
    print(f"DataCite Contributors: {len(contributors)}")
    print(f"Datenbank resourceagent: 6 Einträge")
    print(f"\nDifferenz: {6 - len(creators) - len(contributors)} ungeklärte Einträge")
    
    print("\n" + "=" * 80)
    print("HYPOTHESE")
    print("=" * 80)
    print("Die resourceagent-Tabelle speichert:")
    print("  - Einträge 1-4: DataCite Creators (Autoren)")
    print("  - Eintrag 5: Organisation (HostingInstitution?)")
    print("  - Eintrag 6: Duplikat oder ContactPerson?")
    print("\nWahrscheinlich gibt es eine weitere Tabelle oder ein Mapping,")
    print("das die Rollen (Creator vs. Contributor) speichert.")

if __name__ == "__main__":
    analyze_datacite_metadata()
