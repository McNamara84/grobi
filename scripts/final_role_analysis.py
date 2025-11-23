"""
Finale Analyse: Zeige die vollständige Rolle für jeden resourceagent-Eintrag
"""

import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

def connect_to_database():
    """Verbindung zur SUMARIOPMD-Datenbank"""
    host = os.getenv("DB_SUMARIOPMD_HOST")
    if host and not host.endswith('.gfz-potsdam.de'):
        host = f"{host}.gfz-potsdam.de"
    
    return mysql.connector.connect(
        host=host,
        user=os.getenv("DB_SUMARIOPMD_USER"),
        password=os.getenv("DB_SUMARIOPMD_PASSWORD"),
        database=os.getenv("DB_SUMARIOPMD_NAME")
    )

def analyze_complete_structure():
    """Zeige die vollständige Struktur mit Rollen"""
    conn = connect_to_database()
    cursor = conn.cursor(dictionary=True)
    
    test_doi = "10.5880/gfz_orbit/rso/gnss_g_v02"
    resource_id = 1429
    
    print("=" * 80)
    print(f"VOLLSTÄNDIGE ANALYSE: {test_doi}")
    print(f"resource_id: {resource_id}")
    print("=" * 80)
    
    # Query mit Join auf role-Tabelle
    query = """
        SELECT 
            ra.resource_id,
            ra.`order`,
            ra.name,
            ra.firstname,
            ra.lastname,
            ra.identifier,
            ra.identifiertype,
            ra.nametype,
            GROUP_CONCAT(r.role ORDER BY r.role SEPARATOR ', ') as roles
        FROM resourceagent ra
        LEFT JOIN role r ON 
            r.resourceagent_resource_id = ra.resource_id 
            AND r.resourceagent_order = ra.`order`
        WHERE ra.resource_id = %s
        GROUP BY ra.resource_id, ra.`order`, ra.name, ra.firstname, ra.lastname, 
                 ra.identifier, ra.identifiertype, ra.nametype
        ORDER BY ra.`order`
    """
    
    cursor.execute(query, (resource_id,))
    results = cursor.fetchall()
    
    print(f"\nGefunden: {len(results)} Einträge\n")
    
    for result in results:
        print(f"Eintrag #{result['order']}:")
        print("-" * 80)
        print(f"  Name: {result['name']}")
        if result['firstname']:
            print(f"  Given Name: {result['firstname']}")
        if result['lastname']:
            print(f"  Family Name: {result['lastname']}")
        if result['identifier']:
            print(f"  ORCID: {result['identifier']}")
        if result['roles']:
            print(f"  ROLLEN: {result['roles']}")
        else:
            print(f"  ROLLEN: [KEINE ZUGEORDNET]")
        print()
    
    # Zusammenfassung
    print("=" * 80)
    print("ZUSAMMENFASSUNG")
    print("=" * 80)
    
    creators = [r for r in results if r['roles'] and 'Creator' in r['roles']]
    contributors = [r for r in results if r['roles'] and 'Creator' not in r['roles'] and r['roles'] != 'Creator']
    no_role = [r for r in results if not r['roles']]
    
    print(f"Creators (Autoren): {len(creators)}")
    for c in creators:
        roles = [r for r in c['roles'].split(', ') if r != 'Creator']
        extra = f" + {', '.join(roles)}" if roles else ""
        print(f"  - {c['name']}{extra}")
    
    print(f"\nNur Contributors: {len(contributors)}")
    for c in contributors:
        print(f"  - {c['name']} ({c['roles']})")
    
    print(f"\nOhne Rolle: {len(no_role)}")
    for c in no_role:
        print(f"  - {c['name']}")
    
    print("\n" + "=" * 80)
    print("WICHTIGE ERKENNTNIS")
    print("=" * 80)
    print("Die 'role'-Tabelle definiert die Rolle(n) jedes resourceagent-Eintrags:")
    print("  - Ein Eintrag kann MEHRERE Rollen haben (z.B. Creator + ContactPerson)")
    print("  - Für GROBI-Updates müssen NUR die 'Creator'-Rollen aktualisiert werden")
    print("  - Contributors sollten NICHT verändert werden")
    print("\nFilter für Autoren-Updates:")
    print("  SELECT * FROM resourceagent ra")
    print("  JOIN role r ON r.resourceagent_resource_id = ra.resource_id")
    print("                 AND r.resourceagent_order = ra.`order`")
    print("  WHERE ra.resource_id = ? AND r.role = 'Creator'")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    analyze_complete_structure()
