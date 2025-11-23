"""
Skript zum Finden der Landing Page URLs in den GFZ-Datenbanken.
Durchsucht SUMARIOPMD und MetaWorks Datenbanken nach Landing Page URL-Speicherorten.
"""

import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import csv

# Lade .env Datei
load_dotenv()

def connect_to_database(host, database, user, password):
    """Verbinde mit MySQL-Datenbank"""
    try:
        # Für rz-mysql3 müssen wir möglicherweise .gfz-potsdam.de hinzufügen
        if host == 'rz-mysql3':
            host = 'rz-mysql3.gfz-potsdam.de'
        
        print(f"Versuche Verbindung zu {host} / {database}...")
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            connect_timeout=10
        )
        if connection.is_connected():
            print(f"[OK] Erfolgreich verbunden mit {database}")
            return connection
    except Error as e:
        print(f"[X] Fehler bei Verbindung zu {database}: {e}")
        return None

def get_all_tables(connection, db_name):
    """Hole alle Tabellennamen"""
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    print(f"\n{db_name} hat {len(tables)} Tabellen")
    cursor.close()
    return tables

def search_url_columns(connection, db_name, sample_doi, sample_url):
    """Suche nach Spalten, die URLs enthalten könnten"""
    print(f"\n=== Durchsuche {db_name} nach Landing Page URLs ===")
    
    cursor = connection.cursor()
    tables = get_all_tables(connection, db_name)
    
    results = []
    
    for table in tables:
        try:
            # Hole Spalteninformationen
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            
            # Suche nach potentiellen URL-Spalten
            url_columns = []
            for col in columns:
                col_name = col[0].lower()
                if any(keyword in col_name for keyword in ['url', 'link', 'uri', 'web', 'address', 'location', 'identifier']):
                    url_columns.append(col[0])
            
            if url_columns:
                print(f"\nTabelle: {table}")
                print(f"  Potentielle URL-Spalten: {url_columns}")
                
                # Prüfe Inhalt der URL-Spalten
                for col in url_columns:
                    try:
                        query = f"SELECT {col}, COUNT(*) as cnt FROM {table} WHERE {col} IS NOT NULL GROUP BY {col} LIMIT 10"
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        
                        if rows:
                            print(f"\n  Spalte '{col}' - Beispielwerte:")
                            for row in rows:
                                value = str(row[0])[:100] if row[0] else 'NULL'
                                count = row[1] if len(row) > 1 else 1
                                print(f"    {value} (Anzahl: {count})")
                            
                            # Prüfe ob unsere Beispiel-URL vorhanden ist
                            # Extrahiere die UUID aus der URL
                            if 'id=' in sample_url:
                                url_id = sample_url.split('id=')[1][:36]  # UUID ist 36 Zeichen lang
                                search_query = f"SELECT COUNT(*) FROM {table} WHERE {col} LIKE '%{url_id}%'"
                                cursor.execute(search_query)
                                count = cursor.fetchone()[0]
                                
                                if count > 0:
                                    print(f"    [*] MATCH GEFUNDEN! {count} Einträge enthalten die Beispiel-URL!")
                                    results.append({
                                        'database': db_name,
                                        'table': table,
                                        'column': col,
                                        'matches': count
                                    })
                    except Error as e:
                        print(f"    Fehler beim Abfragen von {col}: {e}")
                        
        except Error as e:
            print(f"  Fehler bei Tabelle {table}: {e}")
    
    cursor.close()
    return results

def search_by_doi(connection, db_name, sample_doi):
    """Suche nach DOI in der Datenbank um relevante Tabellen zu finden"""
    print(f"\n=== Suche nach DOI {sample_doi} in {db_name} ===")
    
    cursor = connection.cursor()
    tables = get_all_tables(connection, db_name)
    
    doi_results = []
    
    for table in tables:
        try:
            # Hole alle Spaltennamen
            cursor.execute(f"DESCRIBE {table}")
            columns = [col[0] for col in cursor.fetchall()]
            
            # Suche DOI in jeder Text-Spalte
            for col in columns:
                try:
                    # Entferne "10." vom DOI für flexiblere Suche
                    doi_search = sample_doi.replace('10.5880/', '')
                    query = f"SELECT * FROM {table} WHERE {col} LIKE '%{doi_search}%' LIMIT 1"
                    cursor.execute(query)
                    row = cursor.fetchone()
                    
                    if row:
                        print(f"\n[OK] DOI gefunden in Tabelle: {table}, Spalte: {col}")
                        print(f"  Spaltennamen der Tabelle: {columns}")
                        
                        # Zeige den kompletten Datensatz
                        cursor.execute(f"SELECT * FROM {table} WHERE {col} LIKE '%{doi_search}%' LIMIT 1")
                        row = cursor.fetchone()
                        
                        print(f"\n  Beispieldatensatz:")
                        for i, col_name in enumerate(columns):
                            value = str(row[i])[:100] if row[i] else 'NULL'
                            print(f"    {col_name}: {value}")
                        
                        doi_results.append({
                            'database': db_name,
                            'table': table,
                            'doi_column': col,
                            'all_columns': columns
                        })
                        break  # Nur ein Match pro Tabelle nötig
                        
                except Error:
                    pass  # Ignoriere Type-Errors bei nicht-text Spalten
                    
        except Error as e:
            print(f"  Fehler bei Tabelle {table}: {e}")
    
    cursor.close()
    return doi_results

def main():
    # Lade Test-DOI und URL aus der CSV
    csv_path = r"c:\Users\Holger\OneDrive\Dokumente\Python-Projekte\grobi\dist\TIB.GFZ_urls.csv"
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        first_row = next(reader)
        sample_doi = first_row['DOI']
        sample_url = first_row['Landing_Page_URL']
    
    print(f"Beispiel-DOI: {sample_doi}")
    print(f"Beispiel-URL: {sample_url}")
    
    # Debug: Zeige geladene Umgebungsvariablen
    print("\n=== Geladene Datenbank-Konfiguration ===")
    print(f"SUMARIOPMD Host: {os.getenv('DB_SUMARIOPMD_HOST')}")
    print(f"SUMARIOPMD DB: {os.getenv('DB_SUMARIOPMD_NAME')}")
    print(f"SUMARIOPMD User: {os.getenv('DB_SUMARIOPMD_USER')}")
    print(f"MetaWorks Host: {os.getenv('DB_METAWORKS_HOST')}")
    print(f"MetaWorks DB: {os.getenv('DB_METAWORKS_NAME')}")
    print(f"MetaWorks User: {os.getenv('DB_METAWORKS_USER')}")
    
    all_results = []
    
    # SUMARIOPMD Datenbank
    conn_sumario = connect_to_database(
        host=os.getenv('DB_SUMARIOPMD_HOST'),
        database=os.getenv('DB_SUMARIOPMD_NAME'),
        user=os.getenv('DB_SUMARIOPMD_USER'),
        password=os.getenv('DB_SUMARIOPMD_PASSWORD')
    )
    
    if conn_sumario:
        # Suche nach URLs
        url_results = search_url_columns(conn_sumario, "SUMARIOPMD", sample_doi, sample_url)
        all_results.extend(url_results)
        
        # Suche nach DOI um die richtige Tabelle zu finden
        doi_results = search_by_doi(conn_sumario, "SUMARIOPMD", sample_doi)
        
        conn_sumario.close()
    
    # MetaWorks Datenbank
    conn_metaworks = connect_to_database(
        host=os.getenv('DB_METAWORKS_HOST'),
        database=os.getenv('DB_METAWORKS_NAME'),
        user=os.getenv('DB_METAWORKS_USER'),
        password=os.getenv('DB_METAWORKS_PASSWORD')
    )
    
    if conn_metaworks:
        # Suche nach URLs
        url_results = search_url_columns(conn_metaworks, "MetaWorks", sample_doi, sample_url)
        all_results.extend(url_results)
        
        # Suche nach DOI um die richtige Tabelle zu finden
        doi_results = search_by_doi(conn_metaworks, "MetaWorks", sample_doi)
        
        conn_metaworks.close()
    
    # Zusammenfassung
    print("\n\n" + "="*80)
    print("ZUSAMMENFASSUNG DER ERGEBNISSE")
    print("="*80)
    
    if all_results:
        print("\n[OK] Landing Page URLs gefunden in:")
        for result in all_results:
            print(f"\n  Datenbank: {result['database']}")
            print(f"  Tabelle: {result['table']}")
            print(f"  Spalte: {result['column']}")
            print(f"  Treffer: {result['matches']}")
    else:
        print("\n[X] Keine direkten Matches für die Landing Page URLs gefunden.")
        print("   Die URLs könnten in einer anderen Form gespeichert sein oder")
        print("   die Tabellen müssen manuell geprüft werden.")

if __name__ == "__main__":
    main()
