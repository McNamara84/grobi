"""
Suche nach Tabellen, die Creator vs. Contributor unterscheiden
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

def search_for_role_tables():
    """Suche nach Tabellen mit Rollen-Information"""
    conn = connect_to_database()
    cursor = conn.cursor(dictionary=True)
    
    print("=" * 80)
    print("SUCHE NACH ROLLEN-TABELLEN")
    print("=" * 80)
    
    # Alle Tabellen anzeigen
    cursor.execute("SHOW TABLES")
    tables = [list(row.values())[0] for row in cursor.fetchall()]
    
    # Suche nach interessanten Tabellen
    role_related = []
    for table in tables:
        table_lower = table.lower()
        if any(keyword in table_lower for keyword in ['role', 'type', 'agent', 'contributor', 'creator', 'relation']):
            role_related.append(table)
    
    print(f"\nGefundene Tabellen mit Rollen-Bezug: {len(role_related)}")
    for table in sorted(role_related):
        print(f"  - {table}")
    
    # Prüfe resourceagent genauer
    print("\n" + "=" * 80)
    print("DETAILANALYSE: resourceagent für resource_id=1429")
    print("=" * 80)
    
    # Alle Spalten mit Daten anzeigen
    cursor.execute("""
        SELECT *
        FROM resourceagent
        WHERE resource_id = 1429
        ORDER BY `order`
    """)
    
    agents = cursor.fetchall()
    print(f"\nAnzahl: {len(agents)}\n")
    
    # Schaue nach Mustern in den Daten
    print("Analyse der nametype-Werte:")
    nametypes = set(a.get('nametype') for a in agents)
    print(f"  Unique values: {nametypes}")
    
    # Suche nach anderen Tabellen, die resource_id=1429 referenzieren
    print("\n" + "=" * 80)
    print("SUCHE NACH VERBUNDENEN TABELLEN")
    print("=" * 80)
    
    related_tables = []
    for table in tables:
        try:
            # Prüfe, ob Tabelle resource_id-Spalte hat
            cursor.execute(f"DESCRIBE `{table}`")
            columns = cursor.fetchall()
            column_names = [c['Field'] for c in columns]
            
            if 'resource_id' in column_names:
                # Prüfe, ob Einträge für resource_id=1429 existieren
                cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table}` WHERE resource_id = 1429")
                count = cursor.fetchone()['cnt']
                if count > 0:
                    related_tables.append((table, count))
        except Exception:
            pass
    
    print("\nTabellen mit resource_id=1429:")
    for table, count in sorted(related_tables):
        print(f"  - {table}: {count} Einträge")
        
        # Zeige Struktur für interessante Tabellen
        if any(keyword in table.lower() for keyword in ['role', 'type', 'agent', 'contributor', 'creator']):
            cursor.execute(f"DESCRIBE `{table}`")
            columns = cursor.fetchall()
            print(f"    Spalten: {', '.join([c['Field'] for c in columns])}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    search_for_role_tables()
