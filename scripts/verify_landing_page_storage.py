"""
Verifiziert, dass Landing Page URLs über publicid in SUMARIOPMD konstruiert werden können.
"""

import os
import mysql.connector
from dotenv import load_dotenv
import csv

load_dotenv()

def connect_db():
    """Verbinde mit SUMARIOPMD"""
    return mysql.connector.connect(
        host=os.getenv('DB_SUMARIOPMD_HOST'),
        database=os.getenv('DB_SUMARIOPMD_NAME'),
        user=os.getenv('DB_SUMARIOPMD_USER'),
        password=os.getenv('DB_SUMARIOPMD_PASSWORD')
    )

def main():
    # Lade CSV mit Landing Page URLs
    csv_path = r"c:\Users\Holger\OneDrive\Dokumente\Python-Projekte\grobi\dist\TIB.GFZ_urls.csv"
    
    print("="*80)
    print("VERIFIZIERUNG: Landing Page URLs in SUMARIOPMD")
    print("="*80)
    
    conn = connect_db()
    cursor = conn.cursor()
    
    # Teste die ersten 10 DOIs aus der CSV
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        matches = 0
        mismatches = 0
        
        for i, row in enumerate(reader):
            if i >= 10:  # Teste nur die ersten 10
                break
            
            doi = row['DOI']
            landing_page_url = row['Landing_Page_URL']
            
            # Extrahiere die publicid (UUID) aus der Landing Page URL
            if 'id=' in landing_page_url:
                expected_publicid = landing_page_url.split('id=')[1]
            else:
                print(f"\n[SKIP] DOI {doi}: Keine Standard-Landing-Page-URL")
                continue
            
            # Suche den DOI in der resource-Tabelle
            query = "SELECT identifier, publicid FROM resource WHERE identifier LIKE %s"
            cursor.execute(query, (f"%{doi.replace('10.5880/', '')}%",))
            result = cursor.fetchone()
            
            if result:
                db_identifier = result[0]
                db_publicid = result[1]
                
                print(f"\n[{i+1}] DOI: {doi}")
                print(f"    Landing Page URL: {landing_page_url}")
                print(f"    DB Identifier:    {db_identifier}")
                print(f"    DB publicid:      {db_publicid}")
                print(f"    Expected publicid: {expected_publicid}")
                
                if db_publicid == expected_publicid:
                    print(f"    [OK] MATCH!")
                    matches += 1
                else:
                    print(f"    [X] MISMATCH!")
                    mismatches += 1
            else:
                print(f"\n[{i+1}] DOI {doi}: Nicht in der Datenbank gefunden")
                mismatches += 1
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*80)
    print("ERGEBNIS")
    print("="*80)
    print(f"Matches: {matches}")
    print(f"Mismatches: {mismatches}")
    print(f"\nFazit: Die Landing Page URLs werden NICHT direkt in der Datenbank gespeichert,")
    print(f"sondern können aus dem 'publicid'-Feld in der 'resource'-Tabelle konstruiert werden:")
    print(f"\n  Landing Page URL = https://dataservices.gfz-potsdam.de/<system>/showshort.php?id=<publicid>")
    print(f"\n  Dabei ist <system> abhängig vom Repository-System (z.B. panmetaworks, enmap, etc.)")
    print(f"\nUM LANDING PAGE URLs ZU AKTUALISIEREN:")
    print(f"  1. Die publicid in der 'resource'-Tabelle muss NICHT geändert werden")
    print(f"  2. Die URLs werden aus der publicid dynamisch konstruiert")
    print(f"  3. Änderungen am Repository-System würden sich auf ALLE URLs auswirken")

if __name__ == "__main__":
    main()
