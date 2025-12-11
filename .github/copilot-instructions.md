# GROBI - Copilot Instructions

## Project Overview

GROBI (**G**FZ **R**esearch Data Repository **O**perations & **B**atch **I**nterface) ist eine PySide6-basierte Desktop-Anwendung für GFZ Data Services zur Verwaltung von DataCite DOIs. Die Anwendung ermöglicht Bulk-Updates von Landing Page URLs und Author-Metadaten.

## Architecture

```
src/
├── main.py              # Entry point, QApplication setup
├── api/
│   └── datacite_client.py   # DataCite REST API v2 client (pagination, error handling)
├── db/
│   └── sumariopmd_client.py # GFZ-internal MySQL database client (PyMySQL)
├── ui/
│   ├── main_window.py       # Main window + inline QObject workers (DOIFetchWorker)
│   ├── credentials_dialog.py # Login dialog with saved accounts
│   ├── theme_manager.py     # AUTO/LIGHT/DARK theme switching
│   └── ...
├── workers/
│   ├── update_worker.py         # URL bulk updates with change detection
│   └── authors_update_worker.py # Author metadata bulk updates
└── utils/
    ├── credential_manager.py # Windows Credential Manager via keyring
    ├── csv_parser.py         # CSV validation (DOI/URL format checks)
    └── csv_exporter.py       # CSV export functions
```

## Key Patterns

### Threading Model
- **Worker Pattern**: Alle API/DB-Operationen laufen in separaten `QThread`s
- Worker sind `QObject`-Klassen mit Signal/Slot-Kommunikation
- Definiere Signals in Worker-Klassen: `progress`, `finished`, `error`, `request_save_credentials`
- Beispiel: `DOIFetchWorker` in `main_window.py`, `UpdateWorker` in `workers/`

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
- **CRITICAL**: Nur `role='Creator'` modifizieren, niemals Contributors

## CSV Formats

**URL Update CSV:**
```csv
DOI,Landing_Page_URL
10.5880/GFZ.xxx,https://dataservices.gfz-potsdam.de/...
```

**Authors CSV:** (8 Spalten)
```csv
DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI
```

## Build Notes

- Nuitka für Windows-Executable
- `pymysql` muss explizit in `main.py` importiert werden (siehe Kommentar dort)
- macOS-Build via `scripts/build_macos.py`
