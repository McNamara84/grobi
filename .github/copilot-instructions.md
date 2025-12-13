# GROBI - Copilot Instructions

## Project Overview

GROBI (**G**FZ **R**esearch Data Repository **O**perations & **B**atch **I**nterface) is a PySide6-based desktop application for GFZ Data Services to manage DataCite DOIs. The application enables:
- Bulk updates of landing page URLs
- Author (Creator) metadata management
- Contributor metadata management with ContactPerson support
- Publisher information updates
- Download URL fetching and export
- CSV file splitting by DOI prefix

## Architecture

```
src/
├── main.py              # Entry point, QApplication setup
├── api/
│   └── datacite_client.py   # DataCite REST API v2 client (pagination, error handling)
├── db/
│   └── sumariopmd_client.py # GFZ-internal MySQL database client (PyMySQL)
├── ui/
│   ├── main_window.py              # Main window + inline QObject workers (DOIFetchWorker, etc.)
│   ├── credentials_dialog.py       # Login dialog with saved accounts
│   ├── save_credentials_dialog.py  # Credential save/update dialog
│   ├── settings_dialog.py          # Database & API settings with connection test
│   ├── csv_splitter_dialog.py      # CSV splitting by DOI prefix
│   ├── about_dialog.py             # About/Version dialog
│   └── theme_manager.py            # AUTO/LIGHT/DARK theme switching
├── workers/
│   ├── update_worker.py                # URL bulk updates with change detection
│   ├── authors_update_worker.py        # Author (Creator) metadata bulk updates
│   ├── contributors_update_worker.py   # Contributor metadata bulk updates
│   ├── publisher_update_worker.py      # Publisher information updates
│   ├── download_url_fetch_worker.py    # Download URL fetching from DataCite
│   └── csv_splitter_worker.py          # CSV splitting by DOI prefix
└── utils/
    ├── credential_manager.py # Windows Credential Manager via keyring
    ├── csv_parser.py         # CSV validation (DOI/URL/Contributors format checks)
    ├── csv_exporter.py       # CSV export functions (DOIs, Authors, Contributors, Download URLs)
    ├── csv_splitter.py       # CSV splitting logic by DOI prefix
    └── publisher_parser.py   # Publisher metadata parsing
```

## Key Patterns

### Threading Model
- **Worker Pattern**: Alle API/DB-Operationen laufen in separaten `QThread`s
- Worker sind `QObject`-Klassen mit Signal/Slot-Kommunikation
- Standard-Signals: `progress_update`, `finished`, `error_occurred`, `request_save_credentials`
- Spezielle Signals für Contributors/Publisher: `validation_update`, `datacite_update`, `database_update`
- Beispiele:
  - Inline Workers: `DOIFetchWorker`, `DOICreatorFetchWorker`, `DOIPublisherFetchWorker`, `DOIContributorFetchWorker` in `main_window.py`
  - Separate Workers: `UpdateWorker`, `AuthorsUpdateWorker`, `ContributorsUpdateWorker`, `PublisherUpdateWorker`, `DownloadURLFetchWorker`, `CSVSplitterWorker` in `workers/`

### Error Handling
- Custom Exceptions pro Modul: `DataCiteAPIError`, `AuthenticationError`, `NetworkError`, `CSVParseError`
- Deutsche Fehlermeldungen für User-facing Errors
- Englische Log-Messages für Debugging

### UI Conventions
- Alle UI-Texte auf Deutsch
- Log-Messages mit `[OK]` oder `[FEHLER]` Prefix für Status
- GroupBox-basierte Workflow-Organisation im MainWindow
- Theme-aware Styling via `ThemeManager`

## Development Commands

```bash
# Run application
python -m src.main

# Run tests
pytest

# Run tests with coverage report
pytest --cov=src --cov-report=html

# Build Windows executable (requires Visual Studio Build Tools)
pip install -r requirements-build.txt
python scripts/build_exe.py
```

## Testing Patterns

- **pytest-qt** für GUI-Tests mit `qtbot` fixture
- **responses** library für HTTP-Mocking (nicht `unittest.mock`)
- Test fixtures in `tests/fixtures/sample_responses.json`
- QApplication-Fixture mit `scope="module"` für Performance:
```python
@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance() or QApplication([])
    yield app
```

## API Integration

### DataCite API
- Production: `https://api.datacite.org`
- Test: `https://api.test.datacite.org`
- Basic Auth mit Client-ID (z.B. `TIB.GFZ`)
- Pagination: `PAGE_SIZE = 100`, automatisches Fetching aller Seiten

### Database (GFZ-internal)
- PyMySQL für MySQL-Verbindung (Nuitka-kompatibel)
- 3-Table Schema: `resource` → `role` → `resourceagent`
- **CRITICAL Database-First Pattern**: 
  - Bei Contributors/Publisher: IMMER zuerst Datenbank aktualisieren, dann DataCite
  - Bei Fehler nach DB-Update: INKONSISTENZ-Marker im Error-Log
  - Bei Authors/Creators: Datenbank NACH erfolgreicher DataCite-Aktualisierung
- Role Types:
  - `role='Creator'`: Authors/Creators (Authors Update Worker)
  - `role='ContactPerson'`: Contributors mit zusätzlichen Feldern (email, website, position)
  - Andere Contributor Types: Nur in DataCite, nicht in DB

## CSV Formats

**URL Update CSV:**
```csv
DOI,Landing_Page_URL
10.5880/GFZ.xxx,https://dataservices.gfz-potsdam.de/...
```

**Authors CSV:** (8 Spalten)
```csv
DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI
10.5880/GFZ.xxx,Mustermann, Max,Personal,Max,Mustermann,0000-0001-2345-6789,ORCID,https://orcid.org
```

**Contributors CSV:** (13 Spalten, optionale Felder für ContactPerson)
```csv
DOI,Contributor Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI,Contributor Type,Email,Website,Position,Affiliation
10.5880/GFZ.xxx,Data Center,Organizational,,,,,,ContactPerson,contact@gfz.de,https://gfz.de,Manager,GFZ
```
- **Contributor Type**: Komma-separierte Liste (z.B. "ContactPerson,DataCollector")
- **ContactPerson**: Benötigt Email + Affiliation, Website/Position optional (nur in DB)
- **Andere Types**: Nur in DataCite, nicht in DB gespeichert

**Publisher CSV:** (2 Spalten)
```csv
DOI,Publisher
10.5880/GFZ.xxx,GFZ Data Services
```

**Download URLs Export:** (automatisch generiert)
```csv
DOI,Download_URL
10.5880/GFZ.xxx,https://dataservices.gfz-potsdam.de/panmetaworks/download/xxx
```

## Build Notes

- Nuitka für Windows-Executable
- `pymysql` muss explizit in `main.py` importiert werden (siehe Kommentar dort)
- macOS-Build via `scripts/build_macos.py`
