"""
Detaillierte Analyse der resourceagent-Tabelle:
- Vollständige Spaltenstruktur
- Unterscheidung zwischen Autoren und Contributors
- Analyse der beiden zusätzlichen Einträge
"""

import mysql.connector
from dotenv import load_dotenv
import os

# .env laden
load_dotenv()

def connect_to_database(db_name):
    """Verbindung zur Datenbank herstellen"""
    if db_name.upper() == 'SUMARIOPMD':
        host = os.getenv("DB_SUMARIOPMD_HOST")
        user = os.getenv("DB_SUMARIOPMD_USER")
        password = os.getenv("DB_SUMARIOPMD_PASSWORD")
        database = os.getenv("DB_SUMARIOPMD_NAME")
    else:
        host = os.getenv("DB_METAWORKS_HOST")
        user = os.getenv("DB_METAWORKS_USER")
        password = os.getenv("DB_METAWORKS_PASSWORD")
        database = os.getenv("DB_METAWORKS_NAME")
    
    if host and not host.endswith('.gfz-potsdam.de'):
        host = f"{host}.gfz-potsdam.de"
    
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

def analyze_table_structure():
    """Analysiere die vollständige Tabellenstruktur"""
    conn = connect_to_database('SUMARIOPMD')
    cursor = conn.cursor(dictionary=True)
    
    print("=" * 80)
    print("VOLLSTÄNDIGE TABELLENSTRUKTUR: resourceagent")
    print("=" * 80)
    
    # Alle Spalten anzeigen
    cursor.execute("DESCRIBE resourceagent")
    columns = cursor.fetchall()
    
    print("\nAlle Spalten:")
    for col in columns:
        print(f"  - {col['Field']:20s} {col['Type']:30s} NULL={col['Null']} Default={col['Default']}")
    
    cursor.close()
    conn.close()

def analyze_test_doi_agents():
    """Detaillierte Analyse aller Einträge für Test-DOI"""
    conn = connect_to_database('SUMARIOPMD')
    cursor = conn.cursor(dictionary=True)
    
    # DOI finden
    test_doi = "10.5880/gfz_orbit/rso/gnss_g_v02"
    cursor.execute("""
        SELECT id, identifier, publicid 
        FROM resource 
        WHERE LOWER(identifier) = LOWER(%s)
    """, (test_doi,))
    
    resource = cursor.fetchone()
    if not resource:
        print(f"[X] DOI nicht gefunden: {test_doi}")
        return
    
    resource_id = resource['id']
    
    print("\n" + "=" * 80)
    print(f"DETAILLIERTE ANALYSE: Alle resourceagent-Einträge für DOI {test_doi}")
    print("=" * 80)
    print(f"resource_id: {resource_id}\n")
    
    # ALLE Spalten abfragen
    cursor.execute("""
        SELECT *
        FROM resourceagent 
        WHERE resource_id = %s
        ORDER BY `order`
    """, (resource_id,))
    
    agents = cursor.fetchall()
    
    print(f"Anzahl Einträge: {len(agents)}\n")
    
    for i, agent in enumerate(agents, 1):
        print(f"Eintrag #{i} (order={agent['order']}):")
        print("-" * 80)
        for key, value in agent.items():
            if value is not None:
                print(f"  {key:20s}: {value}")
        print()
    
    cursor.close()
    conn.close()

def compare_with_datacite():
    """Vergleiche mit DataCite API-Daten"""
    print("=" * 80)
    print("VERGLEICH MIT DATACITE")
    print("=" * 80)
    print("\nDataCite unterscheidet zwischen:")
    print("  - creators (Autoren)")
    print("  - contributors (Contributors mit verschiedenen Typen)")
    print("\nTypische Contributor-Typen:")
    print("  - ContactPerson")
    print("  - DataCollector")
    print("  - DataCurator")
    print("  - DataManager")
    print("  - Distributor")
    print("  - Editor")
    print("  - HostingInstitution")
    print("  - Producer")
    print("  - ProjectLeader")
    print("  - ProjectManager")
    print("  - ProjectMember")
    print("  - RegistrationAgency")
    print("  - RegistrationAuthority")
    print("  - RelatedPerson")
    print("  - Researcher")
    print("  - ResearchGroup")
    print("  - RightsHolder")
    print("  - Sponsor")
    print("  - Supervisor")
    print("  - WorkPackageLeader")
    print("  - Other")

def main():
    analyze_table_structure()
    analyze_test_doi_agents()
    compare_with_datacite()

if __name__ == "__main__":
    main()
