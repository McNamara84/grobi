"""
Detaillierte Analyse der resourceagent-Tabelle in SUMARIOPMD.
Verifiziert die Verbindung zwischen DOI und Autoren-Metadaten.
"""

import os
import mysql.connector
from dotenv import load_dotenv
import csv

load_dotenv()

def connect_db():
    """Verbinde mit SUMARIOPMD"""
    host = os.getenv('DB_SUMARIOPMD_HOST')
    if host == 'rz-mysql3':
        host = 'rz-mysql3.gfz-potsdam.de'
    
    return mysql.connector.connect(
        host=host,
        database=os.getenv('DB_SUMARIOPMD_NAME'),
        user=os.getenv('DB_SUMARIOPMD_USER'),
        password=os.getenv('DB_SUMARIOPMD_PASSWORD')
    )

def main():
    # Lade Test-Daten aus CSV
    csv_path = r"c:\Users\Holger\OneDrive\Dokumente\Python-Projekte\grobi\dist\TIB.GFZ_authors.csv"
    
    print("="*80)
    print("DETAILLIERTE ANALYSE: AUTOREN-METADATEN IN SUMARIOPMD")
    print("="*80)
    
    conn = connect_db()
    cursor = conn.cursor()
    
    # Lade mehrere Test-Autoren
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        test_data = []
        current_doi = None
        
        for i, row in enumerate(reader):
            if i >= 20:  # Erste 20 Zeilen (mehrere DOIs)
                break
            
            if row['DOI'] != current_doi:
                current_doi = row['DOI']
                test_data.append({
                    'doi': row['DOI'],
                    'authors': []
                })
            
            test_data[-1]['authors'].append({
                'name': row['Creator Name'],
                'given': row['Given Name'],
                'family': row['Family Name'],
                'orcid': row['Name Identifier'],
                'orcid_scheme': row['Name Identifier Scheme']
            })
    
    # Teste ersten DOI
    test_doi = test_data[0]['doi']
    test_authors = test_data[0]['authors']
    
    print(f"\nTest-DOI: {test_doi}")
    print(f"Erwartete Autoren: {len(test_authors)}")
    for i, author in enumerate(test_authors, 1):
        print(f"  {i}. {author['given']} {author['family']}")
        if author['orcid']:
            print(f"     ORCID: {author['orcid']}")
    
    # Schritt 1: Finde resource_id für den DOI
    print(f"\n{'='*80}")
    print("SCHRITT 1: Finde resource_id für DOI")
    print(f"{'='*80}")
    
    query = "SELECT id, identifier, publicid, publicstatus FROM resource WHERE identifier LIKE %s"
    cursor.execute(query, (f"%{test_doi.replace('10.5880/', '')}%",))
    resource = cursor.fetchone()
    
    if not resource:
        print("[X] DOI nicht in resource-Tabelle gefunden!")
        conn.close()
        return
    
    resource_id = resource[0]
    print(f"[OK] Gefunden:")
    print(f"  resource.id: {resource_id}")
    print(f"  resource.identifier: {resource[1]}")
    print(f"  resource.publicid: {resource[2]}")
    print(f"  resource.publicstatus: {resource[3]}")
    
    # Schritt 2: Finde alle Autoren für diese resource_id
    print(f"\n{'='*80}")
    print("SCHRITT 2: Finde Autoren für resource_id")
    print(f"{'='*80}")
    
    query = """
        SELECT 
            resource_id,
            `order`,
            name,
            firstname,
            lastname,
            identifier,
            identifiertype,
            nametype
        FROM resourceagent 
        WHERE resource_id = %s
        ORDER BY `order`
    """
    cursor.execute(query, (resource_id,))
    db_authors = cursor.fetchall()
    
    print(f"[OK] Gefunden: {len(db_authors)} Autoren in der Datenbank")
    
    for author in db_authors:
        print(f"\n  Autor #{author[1]}:")  # order
        print(f"    name: {author[2]}")
        print(f"    firstname: {author[3]}")
        print(f"    lastname: {author[4]}")
        print(f"    identifier: {author[5]}")
        print(f"    identifiertype: {author[6]}")
        print(f"    nametype: {author[7]}")
    
    # Schritt 3: Vergleiche mit CSV-Daten
    print(f"\n{'='*80}")
    print("SCHRITT 3: Vergleich CSV ↔ Datenbank")
    print(f"{'='*80}")
    
    print(f"\nAnzahl Autoren:")
    print(f"  CSV: {len(test_authors)}")
    print(f"  Datenbank: {len(db_authors)}")
    
    if len(test_authors) == len(db_authors):
        print("  [OK] Anzahl stimmt überein!")
    else:
        print("  [!] Anzahl unterschiedlich!")
    
    print(f"\nDetail-Vergleich:")
    matches = 0
    for i, (csv_author, db_author) in enumerate(zip(test_authors, db_authors)):
        print(f"\n  Autor #{i+1}:")
        
        # Vergleiche Given Name
        csv_given = csv_author['given']
        db_given = db_author[3]  # firstname
        given_match = csv_given == db_given
        print(f"    Given Name:  CSV='{csv_given}' | DB='{db_given}' | {'[OK]' if given_match else '[X]'}")
        
        # Vergleiche Family Name
        csv_family = csv_author['family']
        db_family = db_author[4]  # lastname
        family_match = csv_family == db_family
        print(f"    Family Name: CSV='{csv_family}' | DB='{db_family}' | {'[OK]' if family_match else '[X]'}")
        
        # Vergleiche ORCID
        csv_orcid = csv_author['orcid']
        db_identifier = db_author[5]  # identifier
        
        if csv_orcid and db_identifier:
            # Extrahiere nur die ORCID-ID
            csv_orcid_id = csv_orcid.split('/')[-1] if '/' in csv_orcid else csv_orcid
            orcid_match = csv_orcid_id in db_identifier or db_identifier in csv_orcid
            print(f"    ORCID:       CSV='{csv_orcid}' | DB='{db_identifier}' | {'[OK]' if orcid_match else '[X]'}")
        elif not csv_orcid and not db_identifier:
            print(f"    ORCID:       Beide leer [OK]")
            orcid_match = True
        else:
            print(f"    ORCID:       CSV='{csv_orcid}' | DB='{db_identifier}' | [X]")
            orcid_match = False
        
        if given_match and family_match:
            matches += 1
    
    print(f"\n{'='*80}")
    print("ERGEBNIS")
    print(f"{'='*80}")
    print(f"Übereinstimmung: {matches}/{len(test_authors)} Autoren")
    
    if matches == len(test_authors):
        print("\n[OK] PERFEKTE ÜBEREINSTIMMUNG!")
        print("\nDatenbank-Schema für Autoren-Updates:")
        print("  Tabelle: resourceagent")
        print("  Primärschlüssel: (resource_id, order)")
        print("  Update-Spalten:")
        print("    - firstname (Given Name)")
        print("    - lastname (Family Name)")
        print("    - identifier (ORCID)")
        print("    - identifiertype (z.B. 'ORCID')")
        print("    - name (Vollständiger Name)")
        print("\nUpdate-SQL-Pattern:")
        print("  UPDATE resourceagent")
        print("  SET firstname = ?, lastname = ?, identifier = ?, identifiertype = ?")
        print("  WHERE resource_id = ? AND `order` = ?")
    else:
        print("\n[!] Nicht alle Autoren stimmen überein - weitere Analyse nötig")
    
    # Teste noch einen zweiten DOI zur Bestätigung
    if len(test_data) > 1:
        print(f"\n{'='*80}")
        print("ZUSÄTZLICHER TEST: Zweiter DOI")
        print(f"{'='*80}")
        
        test_doi2 = test_data[1]['doi']
        test_authors2 = test_data[1]['authors']
        
        print(f"Test-DOI: {test_doi2}")
        print(f"Erwartete Autoren: {len(test_authors2)}")
        
        query = "SELECT id FROM resource WHERE identifier LIKE %s"
        cursor.execute(query, (f"%{test_doi2.replace('10.5880/', '')}%",))
        resource2 = cursor.fetchone()
        
        if resource2:
            resource_id2 = resource2[0]
            query = "SELECT COUNT(*) FROM resourceagent WHERE resource_id = %s"
            cursor.execute(query, (resource_id2,))
            db_count = cursor.fetchone()[0]
            
            print(f"  CSV: {len(test_authors2)} Autoren")
            print(f"  Datenbank: {db_count} Autoren")
            
            if len(test_authors2) == db_count:
                print("  [OK] Auch hier stimmt die Anzahl überein!")
    
    cursor.close()
    conn.close()
    
    print(f"\n{'='*80}")
    print("FAZIT")
    print(f"{'='*80}")
    print("\nDie Autoren-Metadaten sind in der SUMARIOPMD-Datenbank gespeichert:")
    print("\n  Datenbank: sumario-pmd")
    print("  Tabelle: resourceagent")
    print("\n  Beziehung zum DOI:")
    print("    1. DOI in 'resource'-Tabelle → resource.id")
    print("    2. resource.id → resourceagent.resource_id")
    print("    3. Mehrere Autoren pro DOI mit 'order'-Feld für Reihenfolge")
    print("\n  Aktualisierbare Felder:")
    print("    - firstname (Given Name)")
    print("    - lastname (Family Name)")  
    print("    - identifier (ORCID-URL oder ID)")
    print("    - identifiertype (z.B. 'ORCID')")
    print("    - name (Vollständiger Name im Format 'Lastname, Firstname')")
    print("\n  WICHTIG: Die 'order'-Spalte bestimmt die Reihenfolge der Autoren!")

if __name__ == "__main__":
    main()
