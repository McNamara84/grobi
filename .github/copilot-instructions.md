# GitHub Copilot Instructions for GROBI

**GROBI** = **G**FZ **R**esearch Data Repository **O**perations & **B**atch **I**nterface  
A PySide6 (Qt6) desktop application for managing DataCite DOIs at GFZ Data Services.

## Architecture Overview

### Threading Model (CRITICAL)
- **Main thread**: UI operations only (PySide6 widgets, signals, slots)
- **Worker threads**: All API calls and blocking I/O operations
- **Pattern**: `QObject` workers moved to `QThread` instances, communicate via Qt Signals
- **Never**: Block the main thread with API calls or file I/O

Example from `src/workers/update_worker.py`:
```python
class UpdateWorker(QObject):
    progress_update = Signal(int, int, str)
    finished = Signal(int, int, list)
    
    def run(self):
        # Long-running API calls here
        self.progress_update.emit(current, total, message)
```

Usage in `src/ui/main_window.py`:
```python
self.thread = QThread()
self.worker = UpdateWorker(username, password, csv_path, use_test_api)
self.worker.moveToThread(self.thread)
self.thread.started.connect(self.worker.run)
self.worker.finished.connect(self.thread.quit)
```

### Credential Storage
- **Passwords**: Windows Credential Manager via `keyring` library
- **Metadata**: JSON file at `%APPDATA%/GROBI/credentials_metadata.json`
- **Service name**: `GROBI_DataCite`
- **Pattern**: Account ID (UUID) → keyring entry, separate metadata storage
- See `src/utils/credential_manager.py` for implementation

### DataCite API Integration
- **Client**: `src/api/datacite_client.py` handles all API communication
- **Endpoints**: Production (`https://api.datacite.org`) and Test (`https://api.test.datacite.org`)
- **Authentication**: HTTP Basic Auth with username/password
- **Pagination**: Automatic handling of multi-page responses (100 items per page)
- **Error hierarchy**: `DataCiteAPIError` → `AuthenticationError`, `NetworkError`

## Development Workflows

### Running the Application
```powershell
# Always use the virtual environment
.\.venv\Scripts\python.exe -m src.main

# NOT just "python -m src.main" (may use wrong interpreter)
```

### Testing
```powershell
# Run all tests
.\.venv\Scripts\python.exe -m pytest

# With coverage report
.\.venv\Scripts\python.exe -m pytest --cov=src --cov-report=html

# Quick run without verbose output
.\.venv\Scripts\python.exe -m pytest --tb=short -q
```

**Test fixtures**: `tests/fixtures/sample_responses.json` contains mock DataCite API responses  
**Mocking pattern**: Use `responses` library for HTTP mocking, `unittest.mock` for objects

Example from `tests/test_datacite_client.py`:
```python
@responses.activate
def test_single_page_success(self, client):
    responses.add(
        responses.GET,
        "https://api.datacite.org/dois",
        json=FIXTURES["single_page_success"],
        status=200
    )
    dois = client.fetch_all_dois()
    assert len(dois) == 3
```

### Building Windows Executable
```powershell
# Clean previous builds
Remove-Item -Path "dist\GROBI.exe" -Force -ErrorAction SilentlyContinue
Remove-Item -Path "dist\main.build" -Recurse -Force -ErrorAction SilentlyContinue

# Build with Nuitka
python scripts/build_exe.py

# Output: dist/GROBI.exe (~100 MB standalone)
```

**Build requirements**: Visual Studio 2022 Build Tools (MSVC 14.3+)  
**Build time**: First run 10-15 min (caching dependencies), subsequent 5-8 min

### Release Process
1. Update version in `src/__version__.py`
2. Update `CHANGELOG.md` following Keep a Changelog format
3. Commit changes: `git commit -m "Release vX.X.X"`
4. Create and push tag: `git tag vX.X.X && git push origin vX.X.X`
5. GitHub Actions automatically builds EXE and creates release

## Project-Specific Conventions

### Version Management
- **Single source of truth**: `src/__version__.py`
- Used by: About dialog, build scripts, README
- Never hardcode version strings elsewhere

### CSV Validation Patterns
- **DOI format**: `10\.\d+/\S+` (e.g., `10.5880/GFZ.1.1.2021.001`)
- **URL format**: Must start with `http://` or `https://`
- **ORCID format**: Must start with `0000-` (e.g., `0000-0001-5000-0007`)
- See `src/utils/csv_parser.py` for validation logic

### Error Handling Strategy
- **Continue-on-error**: Bulk operations don't fail completely on individual errors
- **Comprehensive logging**: All errors logged with DOI context
- **Update logs**: Timestamped files like `update_log_20251119_143045.txt`
- **User feedback**: Summary dialogs show success/failure counts

### Theme Management
- **Three modes**: AUTO (system detection), LIGHT, DARK
- **Persistence**: Settings stored via QSettings
- **Application**: `ThemeManager.set_theme()` applies stylesheet to entire QApplication
- **Detection**: Windows 10/11 theme auto-detected via registry

### UI Organization Pattern
- **GroupBoxes**: Workflows organized in collapsible groups (Landing Page URLs, Autoren-Metadaten)
- **Status indicators**: ⚪ "Keine CSV-Datei gefunden" / 🟢 "CSV bereit: {filename}"
- **Smart enabling**: Buttons enabled/disabled based on file presence or operation state
- **Menu bar**: Professional desktop app structure (Ansicht, Hilfe)

## Common Pitfalls

### 1. Threading Violations
❌ **NEVER** call API methods from main thread:
```python
# WRONG - blocks UI
client = DataCiteClient(username, password)
dois = client.fetch_all_dois()  # UI freezes!
```

✅ **ALWAYS** use worker threads:
```python
# CORRECT - non-blocking
worker = DOIFetchWorker(client)
worker.moveToThread(self.thread)
self.thread.started.connect(worker.run)
```

### 2. Signal Emission Timing
❌ Emitting signals before moveToThread:
```python
worker = UpdateWorker(...)
worker.progress_update.emit(0, 0, "Starting")  # WRONG - not on thread yet
worker.moveToThread(thread)
```

✅ Emit signals only within `run()` method after thread starts

### 3. CSV Column Names (Authors Update)
- **CRITICAL**: Headers must use spaces: `"Given Name"`, `"Family Name"`, `"Name Identifier"`
- **NOT underscores**: `"Given_Name"` will fail validation
- Export functions use correct format - preserve it when editing CSVs

### 4. Credential Manager Import
```python
# Always check keyring availability
try:
    import keyring
except ImportError:
    keyring = None
    # Handle gracefully with CredentialStorageError
```

### 5. Test API vs Production
- Test credentials: `XUVM.KDVJHQ` (username from test fixtures)
- Production credentials: `TIB.GFZ` (actual GFZ client ID)
- URLs differ: `api.test.datacite.org` vs `api.datacite.org`

## Key Files Reference

- **Entry point**: `src/main.py` - Application setup, logging configuration
- **Main UI**: `src/ui/main_window.py` - 700+ lines, menu bar, workflow groups, thread management
- **API client**: `src/api/datacite_client.py` - 745 lines, pagination, error handling
- **Worker templates**: `src/workers/update_worker.py`, `src/workers/authors_update_worker.py`
- **Credential storage**: `src/utils/credential_manager.py` - Secure keyring integration
- **CSV parsing**: `src/utils/csv_parser.py` - Validation, regex patterns
- **Test fixtures**: `tests/fixtures/sample_responses.json` - Mock API responses
- **Build script**: `scripts/build_exe.py` - Nuitka compilation with MSVC

## Dependencies

**Production** (`requirements.txt`):
- PySide6 ≥6.6.0 (Qt6 GUI framework)
- requests ≥2.31.0 (HTTP client)
- keyring ≥24.3.0 (Windows Credential Manager)

**Development** (`requirements-dev.txt`):
- pytest, pytest-qt, pytest-cov (testing)
- responses (HTTP mocking)
- flake8, black (code quality)

**Build** (`requirements-build.txt`):
- nuitka (Python-to-C compilation)

## Testing Coverage

- **287 unit tests** across 15 test modules
- **77% overall coverage** (business logic 90%+)
- **Pattern**: One test file per source module
- **Fixtures**: Centralized in `tests/fixtures/` directory
- **UI testing**: pytest-qt for QApplication testing
