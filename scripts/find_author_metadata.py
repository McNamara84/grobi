"""
Skript zum Finden der Autoren-Metadaten in den GFZ-Datenbanken.
Durchsucht SUMARIOPMD und MetaWorks nach Speicherorten für:
- Creator Name
- Given Name / Family Name
- Name Identifier (ORCID)
- Name Identifier Scheme
"""

import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import csv

load_dotenv()

def connect_to_database(host, database, user, password):
    """Verbinde mit MySQL-Datenbank"""
    try:
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

def get_all_tables(connection):
    """Hole alle Tabellennamen"""
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    return tables

def search_author_tables(connection, db_name, sample_doi, sample_author):
    """Suche nach Tabellen, die Autoren-Daten enthalten"""
    print(f"\n{'='*80}")
    print(f"Durchsuche {db_name} nach Autoren-Metadaten")
    print(f"{'='*80}")
    print(f"Test-DOI: {sample_doi}")
    print(f"Test-Autor: {sample_author['given']} {sample_author['family']}")
    print(f"Test-ORCID: {sample_author['orcid']}")
    
    cursor = connection.cursor()
    tables = get_all_tables(connection)
    
    print(f"\n{db_name} hat {len(tables)} Tabellen")
    
    results = []
    
    for table in tables:
        try:
            # Hole Spalteninformationen
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            column_names = [col[0] for col in columns]
            
            # Suche nach potentiellen Autoren-Spalten
            author_keywords = ['name', 'author', 'creator', 'agent', 'person', 
                             'given', 'family', 'first', 'last', 'surname',
                             'orcid', 'identifier']
            
            relevant_columns = []
            for col_name in column_names:
                col_lower = col_name.lower()
                if any(keyword in col_lower for keyword in author_keywords):
                    relevant_columns.append(col_name)
            
            if relevant_columns:
                print(f"\n[{table}]")
                print(f"  Relevante Spalten: {', '.join(relevant_columns)}")
                
                # Suche nach dem Test-Autor in den relevanten Spalten
                for col in relevant_columns:
                    try:
                        # Suche nach Given Name
                        if 'given' in col.lower() or 'first' in col.lower():
                            query = f"SELECT * FROM {table} WHERE {col} LIKE %s LIMIT 5"
                            cursor.execute(query, (f"%{sample_author['given']}%",))
                            rows = cursor.fetchall()
                            
                            if rows:
                                print(f"\n  [MATCH in {col}] Gefunden: {len(rows)} Einträge")
                                # Zeige ersten Treffer
                                print(f"    Beispiel-Datensatz:")
                                for i, col_name in enumerate(column_names):
                                    value = str(rows[0][i])[:80] if rows[0][i] else 'NULL'
                                    print(f"      {col_name}: {value}")
                                
                                results.append({
                                    'database': db_name,
                                    'table': table,
                                    'column': col,
                                    'match_type': 'given_name',
                                    'columns': column_names
                                })
                                break  # Ein Match pro Tabelle reicht
                        
                        # Suche nach Family Name
                        elif 'family' in col.lower() or 'last' in col.lower() or 'surname' in col.lower():
                            query = f"SELECT * FROM {table} WHERE {col} LIKE %s LIMIT 5"
                            cursor.execute(query, (f"%{sample_author['family']}%",))
                            rows = cursor.fetchall()
                            
                            if rows:
                                print(f"\n  [MATCH in {col}] Gefunden: {len(rows)} Einträge")
                                print(f"    Beispiel-Datensatz:")
                                for i, col_name in enumerate(column_names):
                                    value = str(rows[0][i])[:80] if rows[0][i] else 'NULL'
                                    print(f"      {col_name}: {value}")
                                
                                results.append({
                                    'database': db_name,
                                    'table': table,
                                    'column': col,
                                    'match_type': 'family_name',
                                    'columns': column_names
                                })
                                break
                        
                        # Suche nach ORCID
                        elif 'orcid' in col.lower() or 'identifier' in col.lower():
                            if sample_author['orcid']:
                                orcid_id = sample_author['orcid'].split('/')[-1]  # Nur die ID
                                query = f"SELECT * FROM {table} WHERE {col} LIKE %s LIMIT 5"
                                cursor.execute(query, (f"%{orcid_id}%",))
                                rows = cursor.fetchall()
                                
                                if rows:
                                    print(f"\n  [MATCH in {col}] Gefunden: {len(rows)} Einträge mit ORCID")
                                    print(f"    Beispiel-Datensatz:")
                                    for i, col_name in enumerate(column_names):
                                        value = str(rows[0][i])[:80] if rows[0][i] else 'NULL'
                                        print(f"      {col_name}: {value}")
                                    
                                    results.append({
                                        'database': db_name,
                                        'table': table,
                                        'column': col,
                                        'match_type': 'orcid',
                                        'columns': column_names
                                    })
                                    break
                    
                    except Error as e:
                        pass  # Ignoriere Type-Errors
                        
        except Error as e:
            if 'syntax error' not in str(e).lower():
                print(f"  Fehler bei Tabelle {table}: {e}")
    
    cursor.close()
    return results

def search_by_doi_and_author(connection, db_name, sample_doi, sample_author):
    """Suche nach DOI UND Autor zusammen"""
    print(f"\n{'='*80}")
    print(f"Suche nach DOI + Autor-Kombination in {db_name}")
    print(f"{'='*80}")
    
    cursor = connection.cursor()
    tables = get_all_tables(connection)
    
    doi_search = sample_doi.replace('10.5880/', '')
    
    for table in tables:
        try:
            cursor.execute(f"DESCRIBE {table}")
            columns = [col[0] for col in cursor.fetchall()]
            
            # Suche nach DOI-Spalte
            doi_col = None
            for col in columns:
                if 'doi' in col.lower() or 'identifier' in col.lower():
                    doi_col = col
                    break
            
            # Suche nach Name-Spalte
            name_col = None
            for col in columns:
                if 'name' in col.lower() or 'family' in col.lower():
                    name_col = col
                    break
            
            if doi_col and name_col:
                try:
                    query = f"""
                        SELECT * FROM {table} 
                        WHERE {doi_col} LIKE %s 
                        AND {name_col} LIKE %s 
                        LIMIT 1
                    """
                    cursor.execute(query, (f"%{doi_search}%", f"%{sample_author['family']}%"))
                    row = cursor.fetchone()
                    
                    if row:
                        print(f"\n[KOMBINIERTE SUCHE - MATCH!]")
                        print(f"  Tabelle: {table}")
                        print(f"  DOI-Spalte: {doi_col}")
                        print(f"  Name-Spalte: {name_col}")
                        print(f"  Alle Spalten: {columns}")
                        print(f"\n  Datensatz:")
                        for i, col in enumerate(columns):
                            value = str(row[i])[:80] if row[i] else 'NULL'
                            print(f"    {col}: {value}")
                
                except Error:
                    pass
        
        except Error:
            pass
    
    cursor.close()

def main():
    # Lade Test-Daten aus der Autoren-CSV
    csv_path = r"c:\Users\Holger\OneDrive\Dokumente\Python-Projekte\grobi\dist\TIB.GFZ_authors.csv"
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        first_row = next(reader)
        
        sample_doi = first_row['DOI']
        sample_author = {
            'given': first_row['Given Name'],
            'family': first_row['Family Name'],
            'orcid': first_row['Name Identifier']
        }
    
    print("="*80)
    print("SUCHE NACH AUTOREN-METADATEN IN GFZ-DATENBANKEN")
    print("="*80)
    print(f"\nTest-Daten:")
    print(f"  DOI: {sample_doi}")
    print(f"  Autor: {sample_author['given']} {sample_author['family']}")
    print(f"  ORCID: {sample_author['orcid']}")
    
    all_results = []
    
    # SUMARIOPMD Datenbank
    conn_sumario = connect_to_database(
        host=os.getenv('DB_SUMARIOPMD_HOST'),
        database=os.getenv('DB_SUMARIOPMD_NAME'),
        user=os.getenv('DB_SUMARIOPMD_USER'),
        password=os.getenv('DB_SUMARIOPMD_PASSWORD')
    )
    
    if conn_sumario:
        results = search_author_tables(conn_sumario, "SUMARIOPMD", sample_doi, sample_author)
        all_results.extend(results)
        
        search_by_doi_and_author(conn_sumario, "SUMARIOPMD", sample_doi, sample_author)
        
        conn_sumario.close()
    
    # MetaWorks Datenbank
    conn_metaworks = connect_to_database(
        host=os.getenv('DB_METAWORKS_HOST'),
        database=os.getenv('DB_METAWORKS_NAME'),
        user=os.getenv('DB_METAWORKS_USER'),
        password=os.getenv('DB_METAWORKS_PASSWORD')
    )
    
    if conn_metaworks:
        results = search_author_tables(conn_metaworks, "MetaWorks", sample_doi, sample_author)
        all_results.extend(results)
        
        search_by_doi_and_author(conn_metaworks, "MetaWorks", sample_doi, sample_author)
        
        conn_metaworks.close()
    
    # Zusammenfassung
    print("\n\n" + "="*80)
    print("ZUSAMMENFASSUNG DER ERGEBNISSE")
    print("="*80)
    
    if all_results:
        print("\nAutoren-Metadaten gefunden in folgenden Tabellen:")
        
        tables_by_db = {}
        for result in all_results:
            db = result['database']
            if db not in tables_by_db:
                tables_by_db[db] = []
            tables_by_db[db].append(result)
        
        for db, results in tables_by_db.items():
            print(f"\n{db}:")
            seen_tables = set()
            for result in results:
                table = result['table']
                if table not in seen_tables:
                    print(f"  - Tabelle: {table}")
                    print(f"    Match-Typ: {result['match_type']}")
                    print(f"    Spalten: {', '.join(result['columns'][:5])}...")
                    seen_tables.add(table)
    else:
        print("\nKeine Autoren-Metadaten in den Standardspalten gefunden.")
        print("Die Daten könnten in spezialisierten Tabellen mit Fremdschlüsseln liegen.")

if __name__ == "__main__":
    main()
