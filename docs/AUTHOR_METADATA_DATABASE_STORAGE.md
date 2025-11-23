# Autoren-Metadaten in GFZ-Datenbanken: Vollständige Analyse

**Datum:** 22. November 2025  
**Datenbank:** SUMARIOPMD (sumario-pmd auf rz-mysql3.gfz-potsdam.de)  
**Kontext:** GROBI soll Autoren-Metadaten nicht nur bei DataCite, sondern auch in der internen GFZ-Datenbank aktualisieren

---

## Zusammenfassung (Executive Summary)

Autoren-Metadaten werden in der SUMARIOPMD-Datenbank in einem **Drei-Tabellen-System** gespeichert:

1. **`resource`** - Enthält DOI und resource_id (Primärschlüssel)
2. **`resourceagent`** - Speichert alle Personen/Organisationen (Autoren UND Contributors)
3. **`role`** - Mapping-Tabelle, die jedem resourceagent-Eintrag eine oder mehrere Rollen zuweist

**Kritische Erkenntnis:** Die `resourceagent`-Tabelle enthält **sowohl Creators (Autoren) als auch Contributors**. Die Unterscheidung erfolgt über die `role`-Tabelle. GROBI darf **nur Einträge mit `role='Creator'` aktualisieren**, nicht aber Contributors wie ContactPerson, Sponsor, etc.

---

## Datenbank-Architektur

### 1. Tabelle: `resource`

Zentrale Tabelle für alle DOI-Metadaten.

```sql
-- Relevante Spalten
id              INT(11)         -- Primärschlüssel, wird als resource_id verwendet
identifier      VARCHAR(255)    -- DOI (z.B. "10.5880/GFZ_ORBIT/RSO/GNSS_G_v02")
publicid        VARCHAR(36)     -- UUID für Landing Page URLs
publicstatus    VARCHAR(20)     -- Status (z.B. "released")
```

**Beziehung:**
- DOI → `resource.id` (resource_id)

### 2. Tabelle: `resourceagent`

Speichert **alle Personen und Organisationen**, die mit einer Ressource verbunden sind (Autoren, Contributors, etc.).

```sql
-- Vollständige Spaltenstruktur
resource_id         INT(11)         NOT NULL    -- Foreign Key zu resource.id
`order`             INT(11)         NOT NULL    -- Reihenfolge (wichtig für Autoren-Sortierung!)
name                VARCHAR(255)    NOT NULL    -- Vollständiger Name ("Lastname, Firstname")
firstname           VARCHAR(255)    NULL        -- Vorname
lastname            VARCHAR(255)    NULL        -- Nachname
identifier          VARCHAR(100)    NULL        -- ORCID-ID (ohne URL-Präfix)
identifiertype      VARCHAR(20)     NULL        -- Z.B. "ORCID"
nametype            VARCHAR(20)     NULL        -- Aktuell nicht genutzt (alle NULL)

-- Composite Primary Key
PRIMARY KEY (resource_id, `order`)
```

**Wichtige Eigenschaften:**
- Ein Eintrag kann **mehrere Rollen** haben (z.B. gleichzeitig Creator UND ContactPerson)
- Das `order`-Feld definiert die **Reihenfolge der Autoren** in der Publikation
- ORCID wird **ohne URL-Präfix** gespeichert: `0000-0001-5401-6794` (nicht `https://orcid.org/...`)
- Organisationen haben oft keinen `firstname`/`lastname`, nur `name`

### 3. Tabelle: `role`

Mapping-Tabelle, die jedem `resourceagent`-Eintrag eine oder mehrere Rollen zuweist.

```sql
-- Vollständige Spaltenstruktur
role                        VARCHAR(25)     -- Foreign Key zu roletype.id
resourceagent_resource_id   INT(11)         -- Foreign Key zu resourceagent.resource_id
resourceagent_order         INT(11)         -- Foreign Key zu resourceagent.order

-- Composite Foreign Key
FOREIGN KEY (resourceagent_resource_id, resourceagent_order) 
    REFERENCES resourceagent(resource_id, `order`)
```

**Wichtige Rollen:**
- **`Creator`** - Autoren (das sind die, die GROBI aktualisiert!)
- `ContactPerson` - Kontaktperson
- `Sponsor` - Sponsor/Förderer
- `DataCollector` - Datensammler
- `Editor` - Editor
- `HostingInstitution` - Hosting-Institution
- ... und viele weitere (siehe `roletype`-Tabelle)

### 4. Tabelle: `roletype` (Referenz)

Lookup-Tabelle mit allen möglichen Rollen (23 verschiedene Typen).

```sql
id              VARCHAR(25)     -- Z.B. "Creator", "ContactPerson"
name            VARCHAR(45)     -- Anzeigename (Z.B. "Author", "Contact Person")
description     TEXT            -- Beschreibung der Rolle
```

---

## Beispiel: Test-DOI Analyse

**DOI:** `10.5880/gfz_orbit/rso/gnss_g_v02`  
**resource_id:** 1429

### resourceagent-Einträge (6 gesamt)

| Order | Name | Firstname | Lastname | ORCID | Rollen |
|-------|------|-----------|----------|-------|--------|
| 1 | Schreiner, Patrick | Patrick | Schreiner | 0000-0001-5401-6794 | **Creator**, ContactPerson |
| 2 | König, Rolf | Rolf | König | 0000-0002-7155-6976 | **Creator** |
| 3 | Neumayer, Karl Hans | Karl Hans | Neumayer | - | **Creator** |
| 4 | Flechtner, Frank | Frank | Flechtner | 0000-0002-3093-5558 | **Creator** |
| 5 | GFZ German Research Centre... | - | - | - | Sponsor |
| 6 | Schreiner, Patrick | - | - | - | pointOfContact |

### Interpretation

- **Einträge 1-4:** Die 4 **Autoren (Creators)** - diese sollen von GROBI aktualisiert werden
- **Eintrag 5:** Organisation als **Sponsor** (Contributor) - NICHT von GROBI ändern!
- **Eintrag 6:** Patrick Schreiner nochmal als **pointOfContact** (Contributor) - NICHT von GROBI ändern!

**Wichtig:** Ein Eintrag kann mehrere Rollen haben (Eintrag #1: Creator + ContactPerson). Beim Filtern nach `role='Creator'` wird dieser Eintrag korrekt als Autor erkannt.

---

## Vergleich: DataCite vs. Datenbank

### DataCite API-Struktur

```json
{
  "data": {
    "attributes": {
      "creators": [
        {
          "name": "Schreiner, Patrick",
          "givenName": "Patrick",
          "familyName": "Schreiner",
          "nameType": "Personal",
          "nameIdentifiers": [
            {
              "nameIdentifier": "https://orcid.org/0000-0001-5401-6794",
              "nameIdentifierScheme": "ORCID"
            }
          ]
        }
      ],
      "contributors": [
        {
          "name": "Schreiner, Patrick",
          "contributorType": "ContactPerson",
          "nameType": "Personal"
        },
        {
          "name": "GFZ German Research Centre for Geosciences, Section 1.2",
          "contributorType": "Sponsor",
          "nameType": "Personal"
        }
      ]
    }
  }
}
```

### Datenbank-Struktur

```sql
-- resourceagent speichert ALLE (Creators + Contributors)
-- role-Tabelle unterscheidet die Rollen
```

### Mapping-Tabelle

| DataCite Feld | Datenbank Tabelle | Datenbank Spalte | Anmerkungen |
|---------------|-------------------|------------------|-------------|
| `creators[].name` | `resourceagent` | `name` | Format: "Lastname, Firstname" |
| `creators[].givenName` | `resourceagent` | `firstname` | - |
| `creators[].familyName` | `resourceagent` | `lastname` | - |
| `creators[].nameIdentifiers[].nameIdentifier` | `resourceagent` | `identifier` | **OHNE** URL-Präfix! |
| `creators[].nameIdentifiers[].nameIdentifierScheme` | `resourceagent` | `identifiertype` | Z.B. "ORCID" |
| `creators[].nameType` | `resourceagent` | `nametype` | Aktuell nicht genutzt (NULL) |
| **Creator vs. Contributor** | `role` | `role` | **'Creator'** für Autoren |
| **Autoren-Reihenfolge** | `resourceagent` | `order` | **Kritisch für Sortierung!** |

---

## SQL-Abfragen für GROBI

### 1. DOI zu resource_id auflösen

```sql
SELECT id, identifier, publicid, publicstatus
FROM resource
WHERE LOWER(identifier) = LOWER(?)
```

**Beispiel:**
```sql
WHERE LOWER(identifier) = LOWER('10.5880/gfz_orbit/rso/gnss_g_v02')
-- Ergebnis: id=1429
```

### 2. Alle Autoren (Creators) für einen DOI abrufen

```sql
SELECT 
    ra.resource_id,
    ra.`order`,
    ra.name,
    ra.firstname,
    ra.lastname,
    ra.identifier,
    ra.identifiertype,
    ra.nametype
FROM resourceagent ra
JOIN role r ON 
    r.resourceagent_resource_id = ra.resource_id 
    AND r.resourceagent_order = ra.`order`
WHERE ra.resource_id = ?
  AND r.role = 'Creator'
ORDER BY ra.`order`
```

**Kritisch:** Der JOIN mit `role` filtert **nur Creators** heraus, keine Contributors!

### 3. Einzelnen Autor aktualisieren

```sql
UPDATE resourceagent
SET 
    firstname = ?,
    lastname = ?,
    identifier = ?,
    identifiertype = ?,
    name = ?
WHERE resource_id = ? 
  AND `order` = ?
```

**Wichtig:** 
- `name` sollte im Format "Lastname, Firstname" gesetzt werden
- `identifier` **ohne** URL-Präfix (nur `0000-0001-5401-6794`)
- `identifiertype` auf `'ORCID'` setzen (falls ORCID vorhanden)

### 4. Neuen Autor hinzufügen

```sql
-- Schritt 1: Maximale order-Nummer ermitteln
SELECT MAX(`order`) as max_order
FROM resourceagent
WHERE resource_id = ?

-- Schritt 2: Neuen Autor einfügen
INSERT INTO resourceagent 
    (resource_id, `order`, name, firstname, lastname, identifier, identifiertype)
VALUES (?, ?, ?, ?, ?, ?, ?)

-- Schritt 3: Creator-Rolle zuweisen
INSERT INTO role 
    (role, resourceagent_resource_id, resourceagent_order)
VALUES ('Creator', ?, ?)
```

**Kritisch:** Beide Tabellen (`resourceagent` UND `role`) müssen aktualisiert werden!

### 5. Autor löschen

```sql
-- Schritt 1: Alle Rollen löschen (Referential Integrity)
DELETE FROM role
WHERE resourceagent_resource_id = ?
  AND resourceagent_order = ?

-- Schritt 2: Autor löschen
DELETE FROM resourceagent
WHERE resource_id = ?
  AND `order` = ?
```

**Wichtig:** Reihenfolge beachten! Zuerst `role`, dann `resourceagent` (wegen Foreign Key).

---

## Datenformat-Unterschiede: DataCite vs. Datenbank

### ORCID-Format

| Quelle | Format | Beispiel |
|--------|--------|----------|
| **DataCite** | Volle URL | `https://orcid.org/0000-0001-5401-6794` |
| **Datenbank** | Nur ID | `0000-0001-5401-6794` |
| **GROBI CSV** | Volle URL | `https://orcid.org/0000-0001-5401-6794` |

**Konvertierung für DB-Update:**
```python
# CSV/DataCite → Datenbank
if orcid_url:
    orcid_id = orcid_url.replace("https://orcid.org/", "")
else:
    orcid_id = None
```

### Name-Format

| Feld | Format | Beispiel |
|------|--------|----------|
| `name` | "Lastname, Firstname" | `"Schreiner, Patrick"` |
| `firstname` | Vorname(n) | `"Patrick"` |
| `lastname` | Nachname | `"Schreiner"` |

**Konstruktion:**
```python
name = f"{lastname}, {firstname}" if firstname and lastname else lastname or firstname
```

---

## Implementierungs-Checkliste für GROBI

### Phase 1: Datenbank-Client erstellen

- [ ] Neue Datei `src/db/sumariopmd_client.py`
- [ ] Klasse `SumarioPMDClient` mit Verbindungsmanagement
- [ ] Credentials aus `.env` laden (wie bei DataCite-Client)
- [ ] Connection Pooling implementieren
- [ ] Error Handling mit spezifischen Exceptions

### Phase 2: Autoren-Synchronisation

- [ ] Funktion `get_resource_id_for_doi(doi: str) -> int`
- [ ] Funktion `fetch_creators_for_resource(resource_id: int) -> List[Dict]`
- [ ] Funktion `update_creator(resource_id: int, order: int, author_data: Dict) -> bool`
- [ ] Funktion `add_creator(resource_id: int, author_data: Dict) -> bool`
- [ ] Funktion `delete_creator(resource_id: int, order: int) -> bool`

### Phase 3: Integration in Authors Update Worker

- [ ] Bestehenden `AuthorsUpdateWorker` erweitern
- [ ] Nach erfolgreichem DataCite-Update → Datenbank-Update triggern
- [ ] Fehlerbehandlung: Bei DB-Fehler trotzdem fortfahren (DataCite ist wichtiger)
- [ ] Logging: Separate Log-Einträge für DataCite vs. Datenbank
- [ ] Progress-Signale erweitern für DB-Status

### Phase 4: Testing

- [ ] Unit-Tests für `SumarioPMDClient`
- [ ] Mock-Tests für DB-Operationen
- [ ] Integration-Test mit Test-Datenbank
- [ ] End-to-End-Test mit echtem DOI (auf Test-API)

### Phase 5: UI-Anpassungen

- [ ] Status-Anzeige: "DataCite ✓ | Datenbank ✓"
- [ ] Fehlermeldungen differenzieren
- [ ] Option zum Deaktivieren von DB-Updates (falls VPN nicht verfügbar)
- [ ] Settings-Dialog: DB-Credentials konfigurierbar machen

---

## Sicherheits-Überlegungen

### 1. Nur Creators aktualisieren

**Gefahr:** Versehentliches Überschreiben von Contributors (ContactPerson, Sponsor, etc.)

**Lösung:** Immer mit JOIN auf `role` filtern:
```sql
WHERE r.role = 'Creator'
```

### 2. Transaktionale Updates

**Gefahr:** Inkonsistenz zwischen `resourceagent` und `role`

**Lösung:** Bei INSERT/DELETE beide Tabellen in einer Transaktion aktualisieren:
```python
try:
    cursor.execute("START TRANSACTION")
    # Update resourceagent
    # Update role
    cursor.execute("COMMIT")
except Exception as e:
    cursor.execute("ROLLBACK")
    raise
```

### 3. Order-Feld Integrität

**Gefahr:** Lücken oder Duplikate in der `order`-Sequenz

**Lösung:** 
- Beim Löschen: `order`-Werte der nachfolgenden Einträge neu nummerieren
- Beim Hinzufügen: Immer `MAX(order) + 1` verwenden

### 4. Datenbankverbindung über VPN

**Gefahr:** GROBI kann nicht verwendet werden, wenn keine VPN-Verbindung besteht

**Lösung:**
- DB-Updates optional machen (Checkbox in UI)
- Graceful Fallback: Bei Verbindungsfehler nur Warnung, kein Abbruch
- Status-Indikator: "DB-Verbindung verfügbar: ✓/✗"

---

## Fehlerbehandlung

### Mögliche Fehlerszenarien

| Fehler | Ursache | Behandlung |
|--------|---------|------------|
| **DOI nicht gefunden** | resource-Tabelle hat keinen Eintrag | Log-Warnung, kein DB-Update |
| **Keine Creators** | Nur Contributors vorhanden | Log-Info, überspringen |
| **Verbindungsfehler** | VPN nicht aktiv | Warnung anzeigen, fortfahren |
| **Permission Denied** | Credentials falsch | Fehler anzeigen, DB-Updates deaktivieren |
| **Transaktion fehlgeschlagen** | Constraint-Verletzung | Rollback, Log-Fehler |

### Empfohlene Error Hierarchy

```python
class SumarioPMDError(Exception):
    """Basis-Exception für SUMARIOPMD-Datenbankfehler"""
    pass

class ConnectionError(SumarioPMDError):
    """Verbindung zur Datenbank fehlgeschlagen"""
    pass

class ResourceNotFoundError(SumarioPMDError):
    """DOI/resource_id nicht in Datenbank gefunden"""
    pass

class TransactionError(SumarioPMDError):
    """Transaktion konnte nicht committed werden"""
    pass
```

---

## Performance-Überlegungen

### 1. Batch-Updates

Bei vielen DOIs sollten DB-Updates gebündelt werden:

```python
# NICHT: Für jeden DOI neue Connection
for doi in dois:
    client = SumarioPMDClient()
    client.update_authors(doi, authors)
    client.close()

# BESSER: Eine Connection für alle
client = SumarioPMDClient()
try:
    for doi in dois:
        client.update_authors(doi, authors)
finally:
    client.close()
```

### 2. Prepared Statements

Wiederholte Queries sollten prepared statements verwenden:

```python
cursor = conn.cursor(prepared=True)
stmt = "UPDATE resourceagent SET firstname=?, lastname=? WHERE resource_id=? AND `order`=?"
for author in authors:
    cursor.execute(stmt, (author.firstname, author.lastname, resource_id, author.order))
```

### 3. Connection Pooling

Für GUI-Anwendung Connection Pool nutzen:

```python
from mysql.connector import pooling

self.pool = pooling.MySQLConnectionPool(
    pool_name="sumariopmd_pool",
    pool_size=5,
    host=host,
    database=database,
    user=user,
    password=password
)
```

---

## Testing-Strategie

### Test-Daten

Verwende die bestehende Test-CSV:
```
dist/TIB.GFZ_authors.csv
```

**Test-DOI:** `10.5880/gfz_orbit/rso/gnss_g_v02` (resource_id=1429)

### Unit-Tests

```python
# tests/test_sumariopmd_client.py

def test_get_resource_id_for_doi(mock_cursor):
    """Test DOI → resource_id Auflösung"""
    client = SumarioPMDClient(...)
    resource_id = client.get_resource_id_for_doi("10.5880/gfz_orbit/...")
    assert resource_id == 1429

def test_fetch_creators_only(mock_cursor):
    """Test dass nur Creators, keine Contributors abgerufen werden"""
    client = SumarioPMDClient(...)
    creators = client.fetch_creators_for_resource(1429)
    assert len(creators) == 4  # Nicht 6!
    assert all(c['role'] == 'Creator' for c in creators)

def test_update_author_with_orcid(mock_cursor):
    """Test ORCID-Format-Konvertierung"""
    client = SumarioPMDClient(...)
    author = {
        'firstname': 'Patrick',
        'lastname': 'Schreiner',
        'orcid': 'https://orcid.org/0000-0001-5401-6794'
    }
    client.update_creator(1429, 1, author)
    # Verify: identifier = '0000-0001-5401-6794' (ohne URL)
```

### Integration-Tests

```python
def test_full_author_update_cycle():
    """Test kompletter Update-Zyklus: Lesen → Ändern → Schreiben → Verifizieren"""
    # 1. Originaldaten lesen
    # 2. Autoren ändern
    # 3. In DB schreiben
    # 4. Erneut auslesen
    # 5. Vergleichen
```

---

## Verifikation

Nach der Implementierung sollte getestet werden:

### 1. Manuelle Verifikation

```sql
-- Vor GROBI-Update
SELECT * FROM resourceagent WHERE resource_id = 1429 ORDER BY `order`;

-- GROBI-Update durchführen (z.B. ORCID ändern)

-- Nach GROBI-Update
SELECT * FROM resourceagent WHERE resource_id = 1429 ORDER BY `order`;

-- Änderungen verifizieren
```

### 2. Verifikations-Skript

Ein Python-Skript ähnlich wie `verify_author_storage.py`, das:
- CSV-Daten lädt
- Datenbank-Daten lädt
- Field-by-Field Vergleich durchführt
- Diskrepanzen reportet

---

## Offene Fragen

### 1. Namensformat-Inkonsistenzen

**Problem:** Eintrag #6 ("Schreiner, Patrick" als pointOfContact) hat keine firstname/lastname.

**Frage:** Soll GROBI diesen Eintrag auch aktualisieren? Oder nur die Creator-Einträge?

**Empfehlung:** Nur Creator-Einträge aktualisieren. Contributors sind separate Entitäten.

### 2. Order-Feld bei Änderungen

**Problem:** Was passiert, wenn Autoren in der CSV umgeordnet werden?

**Optionen:**
- A) `order`-Werte neu vergeben (kompliziert, da in `role`-Tabelle referenziert)
- B) `order` beibehalten, nur Daten aktualisieren (einfacher)

**Empfehlung:** Option B für initiale Implementierung. Warnung, wenn Reihenfolge sich ändert.

### 3. Neue Autoren hinzufügen

**Problem:** Wenn in CSV neue Autoren hinzugefügt werden - wie werden diese in DB eingefügt?

**Lösung:** 
- Neue `order`-Nummer vergeben
- Sowohl `resourceagent` als auch `role` beschreiben
- Transaktional durchführen

---

## Zusammenfassung: Kritische Punkte

1. **✓ Nur Creators aktualisieren** - Immer `JOIN role WHERE role='Creator'` verwenden
2. **✓ ORCID-Format konvertieren** - URL-Präfix entfernen für DB-Speicherung
3. **✓ Order-Feld respektieren** - Definiert Autoren-Reihenfolge
4. **✓ Transaktional arbeiten** - Bei INSERT/DELETE beide Tabellen atomar aktualisieren
5. **✓ Error Handling** - DB-Fehler dürfen DataCite-Update nicht blockieren
6. **✓ VPN-Abhängigkeit** - Graceful Fallback wenn DB nicht erreichbar

---

## Nächste Schritte

1. **Dokumentation Review** - Diese Dokumentation von Holger reviewen lassen
2. **DB-Client implementieren** - `src/db/sumariopmd_client.py` erstellen
3. **Unit-Tests schreiben** - Alle DB-Operationen testen
4. **Integration in Worker** - `AuthorsUpdateWorker` erweitern
5. **UI anpassen** - Status-Anzeigen für DB-Updates
6. **Testing** - End-to-End-Tests mit echten Daten
7. **Deployment** - Credentials in Produktions-Umgebung konfigurieren

---

**Erstellt am:** 22. November 2025  
**Version:** 1.0  
**Autor:** GitHub Copilot (mit Holger McNamara)
