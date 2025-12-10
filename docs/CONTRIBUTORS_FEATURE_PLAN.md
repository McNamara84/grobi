# GROBI Contributors Feature - Implementierungsplan

**Feature Request:** Export und Update von Contributor-Metadaten analog zum bestehenden Autoren-Workflow  
**Branch:** `feature/load-and-update-contributors`  
**Erstellt:** 30. November 2025

---

## 1. Zusammenfassung

Dieses Feature ermöglicht den Export und Import von Contributor-Daten (alle Nicht-Creator-Rollen) aus/in DataCite und der internen Datenbank. Ein Contributor kann mehrere Rollen haben, und ContactPerson-Contributor haben zusätzlich Email/Website-Daten in der internen Datenbank.

---

## 2. Datenbankschema-Analyse

### 2.1 Relevante Tabellen

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              resource                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ id (PK) │ identifier (DOI) │ publisher │ updated_at                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ 1
                                    │
                                    ▼ N
┌─────────────────────────────────────────────────────────────────────────────┐
│                            resourceagent                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│ resource_id (PK,FK) │ order (PK) │ name │ firstname │ lastname │            │
│ identifier (ORCID)  │ identifiertype │ nametype                              │
└─────────────────────────────────────────────────────────────────────────────┘
          │ 1                                              │ 1
          │                                                │
          ▼ N                                              ▼ 0..1
┌─────────────────────────────┐            ┌───────────────────────────────────┐
│           role              │            │          contactinfo               │
├─────────────────────────────┤            ├───────────────────────────────────┤
│ role (PK)                   │            │ resourceagent_resource_id (PK,FK) │
│ resourceagent_resource_id   │            │ resourceagent_order (PK,FK)       │
│ resourceagent_order         │            │ email │ website │ position        │
└─────────────────────────────┘            └───────────────────────────────────┘
```

### 2.2 Rollentypen in der Datenbank

| Rolle               | Anzahl  | DataCite ContributorType |
|---------------------|---------|--------------------------|
| Creator             | 12.939  | *(separat, nicht Contributor)* |
| pointOfContact      | 2.743   | ContactPerson (GFZ-intern) |
| ProjectMember       | 2.426   | ProjectMember |
| DataCollector       | 1.839   | DataCollector |
| Researcher          | 1.478   | Researcher |
| ContactPerson       | 1.449   | ContactPerson |
| DataManager         | 1.061   | DataManager |
| DataCurator         | 1.047   | DataCurator |
| Supervisor          | 1.011   | Supervisor |
| HostingInstitution  | 806     | HostingInstitution |
| ... (weitere)       | ...     | ... |

**Wichtig:** Ein `resourceagent` kann mehrere Rollen haben (z.B. Creator + ContactPerson).

### 2.3 ContactInfo-Statistiken

- **Total Einträge:** 3.477
- **Mit Email:** 3.476 (99,97%)
- **Mit Website:** 3.264 (93,87%)
- **Mit Position:** 3.311 (95,23%)

---

## 3. Feature-Spezifikation

### 3.1 CSV-Format für Contributors

```csv
DOI,Contributor Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI,Contributor Types,Email,Website,Position
10.5880/GFZ.1.1.2021.001,Müller, Hans,Personal,Hans,Müller,https://orcid.org/0000-0001-...,ORCID,https://orcid.org,"ContactPerson, DataManager",hans.mueller@gfz.de,https://www.gfz.de,Senior Scientist
10.5880/GFZ.1.1.2021.001,GFZ Data Services,Organizational,,,https://ror.org/04z8jg394,ROR,https://ror.org,HostingInstitution,,,
```

**Spalten:**
1. `DOI` - DOI-Identifier
2. `Contributor Name` - Vollständiger Name (Format: "Nachname, Vorname" oder Organisationsname)
3. `Name Type` - "Personal" oder "Organizational"
4. `Given Name` - Vorname (nur bei Personal)
5. `Family Name` - Nachname (nur bei Personal)
6. `Name Identifier` - ORCID/ROR/ISNI URL
7. `Name Identifier Scheme` - "ORCID", "ROR", "ISNI" etc.
8. `Scheme URI` - Schema-URI
9. `Contributor Types` - **Kommaseparierte Liste** der Contributor-Typen
10. `Email` - E-Mail-Adresse (nur DB, nur wenn ContactPerson)
11. `Website` - Website-URL (nur DB, nur wenn ContactPerson)
12. `Position` - Position/Titel (nur DB, nur wenn ContactPerson)

### 3.2 Unterstützte ContributorTypes (DataCite 4.6)

Alle 21 DataCite ContributorTypes werden unterstützt:
- ContactPerson, DataCollector, DataCurator, DataManager
- Distributor, Editor, HostingInstitution, Producer
- ProjectLeader, ProjectManager, ProjectMember
- RegistrationAgency, RegistrationAuthority, RelatedPerson
- Researcher, ResearchGroup, RightsHolder, Sponsor
- Supervisor, Translator, WorkPackageLeader, Other

### 3.3 Workflow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           EXPORT WORKFLOW                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│ 1. User klickt "DOIs und Contributors exportieren"                            │
│ 2. Login-Dialog (DataCite Credentials)                                        │
│ 3. Fetch DOIs mit Contributors von DataCite API                               │
│ 4. Für jeden Contributor mit ContactPerson-Rolle:                             │
│    → Fetch Email/Website/Position aus DB (contactinfo-Tabelle)                │
│ 5. Export als CSV: {username}_contributors.csv                                │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                           UPDATE WORKFLOW                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│ 1. User klickt "Contributors aktualisieren"                                   │
│ 2. CSV-Datei auswählen                                                        │
│ 3. Login-Dialog (DataCite Credentials)                                        │
│ 4. VALIDIERUNGSPHASE:                                                         │
│    a) Parse CSV, validiere Format                                             │
│    b) Teste DataCite API Verbindung                                           │
│    c) Teste DB Verbindung (wenn aktiviert)                                    │
│    d) Dry-Run: Prüfe ob DOIs existieren, vergleiche Contributor-Anzahl        │
│ 5. UPDATE-PHASE (DB-First Pattern):                                           │
│    a) Update Datenbank:                                                       │
│       - resourceagent (name, firstname, lastname, identifier, ...)            │
│       - role (alle Contributor-Rollen)                                        │
│       - contactinfo (email, website, position - nur bei ContactPerson)        │
│    b) Update DataCite API:                                                    │
│       - contributors Array (ohne Email/Website)                               │
│ 6. Log-Datei mit Ergebnissen                                                  │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Implementierungs-Roadmap

### Phase 1: API & Datenbank-Layer (Geschätzter Aufwand: 4-6 Stunden)

#### 4.1.1 DataCite Client erweitern (`src/api/datacite_client.py`)

**Neue Methoden:**
```python
def fetch_all_dois_with_contributors(self) -> List[Tuple[...]]
    """Fetch all DOIs with contributor information."""
    
def _fetch_page_with_contributors(self, page_number: int) -> Tuple[List[...], bool]
    """Fetch a single page of DOIs with contributor data."""
    
def validate_contributors_match(self, doi: str, csv_contributors: List[Dict]) -> Tuple[bool, str]
    """Validate CSV contributors against current DataCite metadata."""
    
def update_doi_contributors(self, doi: str, new_contributors: List[Dict], current_metadata: Dict) -> Tuple[bool, str]
    """Update contributor metadata for a DOI."""
```

**Contributor-Datenstruktur von DataCite API:**
```json
{
  "contributors": [
    {
      "name": "Garcia, Sofia",
      "nameType": "Personal",
      "givenName": "Sofia",
      "familyName": "Garcia",
      "contributorType": "DataCollector",
      "nameIdentifiers": [
        {
          "nameIdentifier": "https://orcid.org/0000-0001-...",
          "nameIdentifierScheme": "ORCID",
          "schemeUri": "https://orcid.org"
        }
      ],
      "affiliation": [...]  // Wird ignoriert (per Anforderung)
    }
  ]
}
```

#### 4.1.2 Datenbank Client erweitern (`src/db/sumariopmd_client.py`)

**Neue Methoden:**
```python
def fetch_contributors_for_resource(self, resource_id: int) -> List[Dict[str, Any]]
    """
    Fetch all Contributors (non-Creator roles) for a resource.
    Returns list with: order, name, firstname, lastname, orcid, identifiertype, 
                       nametype, roles (comma-separated), email, website, position
    """
    
def fetch_contactinfo_for_contributor(self, resource_id: int, order: int) -> Optional[Dict]
    """Fetch email/website/position for a specific contributor."""
    
def update_contributors_transactional(self, resource_id: int, contributors: List[Dict]) -> Tuple[bool, str, List[str]]
    """
    Update contributors for a resource in a transaction.
    
    Steps:
    1. BEGIN TRANSACTION
    2. Delete existing non-Creator entries (resourceagent + role + contactinfo)
    3. Insert new contributor entries
    4. Insert role entries (multiple per contributor)
    5. Insert contactinfo entries (only for ContactPerson)
    6. COMMIT/ROLLBACK
    """
    
def upsert_contactinfo(self, resource_id: int, order: int, email: str, website: str, position: str) -> bool
    """Insert or update contactinfo for a contributor."""
```

**SQL-Queries:**
```sql
-- Fetch contributors with all roles and contactinfo
SELECT 
    ra.order,
    ra.name,
    ra.firstname,
    ra.lastname,
    ra.identifier AS orcid,
    ra.identifiertype,
    ra.nametype,
    GROUP_CONCAT(DISTINCT r.role ORDER BY r.role SEPARATOR ', ') AS roles,
    ci.email,
    ci.website,
    ci.position
FROM resourceagent ra
INNER JOIN role r 
    ON r.resourceagent_resource_id = ra.resource_id 
    AND r.resourceagent_order = ra.order
LEFT JOIN contactinfo ci 
    ON ci.resourceagent_resource_id = ra.resource_id 
    AND ci.resourceagent_order = ra.order
WHERE ra.resource_id = %s 
    AND r.role != 'Creator'
GROUP BY ra.resource_id, ra.order, ra.name, ra.firstname, ra.lastname,
         ra.identifier, ra.identifiertype, ra.nametype,
         ci.email, ci.website, ci.position
ORDER BY ra.order ASC
```

### Phase 2: CSV Parser & Exporter (Geschätzter Aufwand: 2-3 Stunden)

#### 4.2.1 CSV Parser erweitern (`src/utils/csv_parser.py`)

**Neue Methode:**
```python
@staticmethod
def parse_contributors_update_csv(filepath: str) -> Tuple[Dict[str, List[Dict]], List[str]]
    """
    Parse CSV file with contributor data.
    
    Expected headers:
    DOI, Contributor Name, Name Type, Given Name, Family Name,
    Name Identifier, Name Identifier Scheme, Scheme URI,
    Contributor Types, Email, Website, Position
    
    Validierungen:
    - DOI-Format prüfen
    - Contributor Types gegen erlaubte Werte prüfen
    - Email-Format prüfen (optional)
    - URL-Format für Website prüfen (optional)
    """
```

#### 4.2.2 CSV Exporter erweitern (`src/utils/csv_exporter.py`)

**Neue Funktion:**
```python
def export_dois_with_contributors_to_csv(
    data: List[Tuple[str, str, str, str, str, str, str, str, str, str, str, str]],
    username: str,
    output_dir: str = None
) -> str
    """
    Export DOIs with contributor information to CSV.
    
    Columns:
    DOI, Contributor Name, Name Type, Given Name, Family Name,
    Name Identifier, Name Identifier Scheme, Scheme URI,
    Contributor Types, Email, Website, Position
    
    Returns: Path to created CSV file ({username}_contributors.csv)
    """
```

### Phase 3: Worker Threads (Geschätzter Aufwand: 4-5 Stunden)

#### 4.3.1 Neuer Fetch Worker (`src/ui/main_window.py`)

```python
class DOIContributorFetchWorker(QObject):
    """Worker for fetching DOIs with contributor information."""
    
    # Signals
    progress = Signal(str)
    finished = Signal(list, str)  # contributor data, username
    error = Signal(str)
    request_save_credentials = Signal(str, str, str)
    
    def run(self):
        """
        1. Fetch DOIs with contributors from DataCite
        2. For each contributor with ContactPerson role:
           → Fetch email/website/position from DB
        3. Merge data and emit finished signal
        """
```

#### 4.3.2 Neuer Update Worker (`src/workers/contributors_update_worker.py`)

```python
class ContributorsUpdateWorker(QObject):
    """Worker for updating DOI contributor metadata."""
    
    # Signals (analog zu AuthorsUpdateWorker)
    progress_update = Signal(int, int, str)
    dry_run_complete = Signal(int, int, list)
    doi_updated = Signal(str, bool, str)
    finished = Signal(int, int, int, list, list)
    error_occurred = Signal(str)
    request_save_credentials = Signal(str, str, str)
    validation_update = Signal(str)
    datacite_update = Signal(str)
    database_update = Signal(str)
    
    def run(self):
        """
        Hauptablauf:
        1. CSV parsen
        2. DataCite Client initialisieren
        3. DB Client initialisieren (wenn aktiviert)
        4. Validierungsphase (Dry Run)
        5. Update-Phase (DB-First Pattern):
           a) Für jeden DOI:
              - DB Update (resourceagent, role, contactinfo)
              - DataCite Update (contributors array)
        6. Log-Datei schreiben
        """
    
    def _detect_contributor_changes(self, current_metadata: dict, csv_contributors: list) -> Tuple[bool, str]
        """Compare current DataCite metadata with CSV data."""
    
    def _build_contributor_payload(self, csv_contributors: list) -> list
        """Build DataCite contributors array from CSV data."""
    
    def _update_database_contributors(self, doi: str, contributors: list) -> Tuple[bool, str]
        """Update contributors in database (resourceagent + role + contactinfo)."""
```

### Phase 4: UI Integration (Geschätzter Aufwand: 3-4 Stunden)

#### 4.4.1 Neue GroupBox in MainWindow (`src/ui/main_window.py`)

```python
def _create_contributors_group(self) -> QGroupBox:
    """
    Create the Contributors group box.
    
    Layout:
    ┌─────────────────────────────────────────────────────────────┐
    │ 👥 Contributors                                              │
    ├─────────────────────────────────────────────────────────────┤
    │ [📥 DOIs und Contributors exportieren]                       │
    │                                                              │
    │ [🔄 Contributors aktualisieren]                              │
    └─────────────────────────────────────────────────────────────┘
    """
```

**Neue Buttons:**
- `📥 DOIs und Contributors exportieren` → Startet `DOIContributorFetchWorker`
- `🔄 Contributors aktualisieren` → Startet `ContributorsUpdateWorker`

#### 4.4.2 Slot-Methoden

```python
def _on_load_contributors_clicked(self):
    """Handle 'Load Contributors' button click."""
    
def _on_update_contributors_clicked(self):
    """Handle 'Update Contributors' button click."""
    
def _on_contributors_fetch_finished(self, contributor_data: list, username: str):
    """Handle completed contributor fetch - export to CSV."""
    
def _on_contributors_update_finished(self, success: int, errors: int, skipped: int, ...):
    """Handle completed contributor update."""
```

### Phase 5: Tests (Geschätzter Aufwand: 4-6 Stunden)

#### 4.5.1 Neue Test-Dateien

```
tests/
├── test_datacite_client_contributors.py    # API Tests für fetch/update
├── test_sumariopmd_contributors.py         # DB Tests für CRUD-Operationen
├── test_csv_parser_contributors.py         # CSV Parsing Tests
├── test_csv_exporter_contributors.py       # CSV Export Tests
├── test_contributors_update_worker.py      # Worker Integration Tests
└── fixtures/
    └── sample_contributors.json            # Test-Daten
```

#### 4.5.2 Test-Szenarien

1. **CSV Parser Tests:**
   - Valides CSV mit allen Spalten
   - CSV mit fehlenden optionalen Spalten (Email, Website)
   - CSV mit mehreren Contributor Types pro Zeile
   - Ungültige Contributor Types
   - Ungültige Email-Formate

2. **DataCite API Tests (mit `responses` Library):**
   - Fetch contributors für DOI
   - Update contributors (success/error cases)
   - Change detection

3. **Database Tests:**
   - Fetch contributors mit mehreren Rollen
   - Update mit contactinfo
   - Transaktions-Rollback bei Fehler

4. **Worker Tests:**
   - Dry-Run Validierung
   - Update-Workflow mit DB-First Pattern
   - Error Handling

---

## 5. Detaillierte Änderungsliste

### 5.1 Neue Dateien

| Datei | Beschreibung |
|-------|--------------|
| `src/workers/contributors_update_worker.py` | Worker für Contributor-Updates |
| `tests/test_datacite_client_contributors.py` | API Tests |
| `tests/test_sumariopmd_contributors.py` | DB Tests |
| `tests/test_csv_parser_contributors.py` | Parser Tests |
| `tests/test_csv_exporter_contributors.py` | Exporter Tests |
| `tests/test_contributors_update_worker.py` | Worker Tests |

### 5.2 Geänderte Dateien

| Datei | Änderungen |
|-------|------------|
| `src/api/datacite_client.py` | +4 neue Methoden für Contributors |
| `src/db/sumariopmd_client.py` | +4 neue Methoden für Contributors/ContactInfo |
| `src/utils/csv_parser.py` | +1 neue Methode `parse_contributors_update_csv()` |
| `src/utils/csv_exporter.py` | +1 neue Funktion `export_dois_with_contributors_to_csv()` |
| `src/ui/main_window.py` | +1 Worker-Klasse, +1 GroupBox, +6 Slot-Methoden |
| `README.md` | Dokumentation für Contributors-Feature |
| `CHANGELOG.md` | Feature-Eintrag |

---

## 6. Risiken & Mitigation

| Risiko | Wahrscheinlichkeit | Mitigation |
|--------|-------------------|------------|
| Komplexität durch mehrere Rollen pro Contributor | Hoch | Klares Datenmodell, extensive Tests |
| DB-Schema Inkompatibilität | Niedrig | Schema bereits validiert |
| Performance bei vielen DOIs | Mittel | Pagination, Progress-Feedback |
| Transaktions-Fehler bei DB-Updates | Mittel | ROLLBACK-Mechanismus, atomare Operationen |

---

## 7. Zeitschätzung

| Phase | Aufwand | Kumuliert |
|-------|---------|-----------|
| Phase 1: API & DB Layer | 4-6 Std. | 4-6 Std. |
| Phase 2: CSV Parser/Exporter | 2-3 Std. | 6-9 Std. |
| Phase 3: Worker Threads | 4-5 Std. | 10-14 Std. |
| Phase 4: UI Integration | 3-4 Std. | 13-18 Std. |
| Phase 5: Tests | 4-6 Std. | 17-24 Std. |
| **Gesamt** | **17-24 Stunden** | |

---

## 8. Abnahmekriterien

- [ ] Export von Contributors zu CSV funktioniert
- [ ] CSV enthält alle ContributorTypes als kommaseparierte Liste
- [ ] Email/Website/Position werden für ContactPerson aus DB geladen
- [ ] Import von Contributors aus CSV funktioniert
- [ ] Dry-Run Validierung zeigt korrekte Ergebnisse
- [ ] DB-Updates erfolgen vor DataCite-Updates (DB-First Pattern)
- [ ] Bei DB-Fehler: Kein DataCite-Update
- [ ] Email/Website nur in DB aktualisiert, nicht bei DataCite
- [ ] Affiliations werden nicht exportiert/importiert (unverändert)
- [ ] Alle Tests bestehen
- [ ] Code-Coverage mindestens 80%
