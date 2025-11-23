"""
Analysiere die role und roletype Tabellen
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

def analyze_role_tables():
    """Analysiere role und roletype Tabellen"""
    conn = connect_to_database()
    cursor = conn.cursor(dictionary=True)
    
    # roletype Tabelle
    print("=" * 80)
    print("TABELLE: roletype")
    print("=" * 80)
    
    cursor.execute("DESCRIBE roletype")
    columns = cursor.fetchall()
    print("\nSpalten:")
    for col in columns:
        print(f"  - {col['Field']:20s} {col['Type']:30s}")
    
    cursor.execute("SELECT * FROM roletype")
    roletypes = cursor.fetchall()
    print(f"\nAlle Einträge ({len(roletypes)}):")
    for rt in roletypes:
        print(f"  {rt}")
    
    # role Tabelle
    print("\n" + "=" * 80)
    print("TABELLE: role")
    print("=" * 80)
    
    cursor.execute("DESCRIBE role")
    columns = cursor.fetchall()
    print("\nSpalten:")
    for col in columns:
        print(f"  - {col['Field']:20s} {col['Type']:30s}")
    
    cursor.execute("SELECT COUNT(*) as cnt FROM role")
    count = cursor.fetchone()['cnt']
    print(f"\nGesamtanzahl Einträge: {count}")
    
    # Prüfe, ob role einen Bezug zu resourceagent hat
    cursor.execute("SELECT * FROM role LIMIT 10")
    sample_roles = cursor.fetchall()
    print("\nErste 10 Einträge:")
    for r in sample_roles:
        print(f"  {r}")
    
    # Prüfe, ob es einen Link zu resource_id=1429 gibt
    print("\n" + "=" * 80)
    print("VERBINDUNG ZU UNSEREM TEST-DOI (resource_id=1429)")
    print("=" * 80)
    
    # Prüfe, ob role-Tabelle resource_id hat
    cursor.execute("DESCRIBE role")
    role_columns = [c['Field'] for c in cursor.fetchall()]
    print(f"\nSpalten in 'role': {', '.join(role_columns)}")
    
    # Wenn agent_id existiert, versuche Join
    if 'agent_id' in role_columns:
        print("\nVersuche Join: role → agent → resourceagent...")
        
        # Prüfe agent-Tabelle
        cursor.execute("DESCRIBE agent")
        agent_columns = [c['Field'] for c in cursor.fetchall()]
        print(f"Spalten in 'agent': {', '.join(agent_columns)}")
        
        # Versuche komplexen Join
        query = """
            SELECT 
                ra.resource_id,
                ra.`order`,
                ra.name,
                ra.firstname,
                ra.lastname,
                a.id as agent_id,
                r.id as role_id,
                rt.name as role_name
            FROM resourceagent ra
            LEFT JOIN agent a ON ra.name = a.name
            LEFT JOIN role r ON a.id = r.agent_id
            LEFT JOIN roletype rt ON r.roletype_id = rt.id
            WHERE ra.resource_id = 1429
            ORDER BY ra.`order`
        """
        
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            
            print(f"\nJoin-Ergebnis ({len(results)} Zeilen):")
            for row in results:
                print(f"  Order {row['order']}: {row['name']}")
                print(f"    agent_id={row['agent_id']}, role_id={row['role_id']}, role_name={row['role_name']}")
        except Exception as e:
            print(f"[X] Join fehlgeschlagen: {e}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    analyze_role_tables()
