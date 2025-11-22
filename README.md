# GROBI

![Tests](https://github.com/McNamara84/grobi/workflows/Tests/badge.svg)
[![codecov](https://codecov.io/gh/McNamara84/grobi/branch/main/graph/badge.svg)](https://codecov.io/gh/McNamara84/grobi)
![Python](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)
![License](https://img.shields.io/badge/license-GPLv3-green)

**G**FZ **R**esearch Data Repository **O**perations & **B**atch **I**nterface

A modern GUI tool for GFZ Data Services to manage DataCite DOIs.

## Features

### DOI Management
- 🔍 **Retrieve DOIs**: Fetch all registered DOIs from DataCite API
- 📊 **Export to CSV**: Export DOI list with landing page URLs
- 👥 **Export Authors**: Export DOI list with creator/author information (ORCID support)
- 🔄 **Update Landing Page URLs**: Bulk update landing page URLs via CSV import
- 🖊️ **Update Authors**: Bulk update creator/author metadata with dry run validation
- 📝 **Detailed Logging**: Automatic creation of update logs with success/error reports

### Technical Features
- 🧪 Support for test and production API
- 🎨 Modern user interface with PySide6 (Qt6)
- 🗺️ **Professional Menubar**: Organized menus for theme settings and help resources
- 💬 **About Dialog**: Displays version, author info, and quick access to GitHub, Changelog, and License
- 🏭 **Prominent Logo**: Application logo displayed in main window header
- 📦 **Workflow Organization**: GroupBox-based UI with status indicators for CSV file availability
- 🌓 **Dark Mode Support**: Auto-detection of system theme with manual override (AUTO/LIGHT/DARK)
- 🔐 **Credential Management**: Secure storage of multiple DataCite accounts with Windows Credential Manager
- ⚡ Non-blocking API calls with progress indication
- 📈 Real-time progress tracking for bulk operations
- 🛡️ Comprehensive error handling and validation
- 📋 CSV format validation (DOI and URL format checks)
- 💾 Persistent theme preference across sessions

## Download

**[📥 Download Latest Release](https://github.com/McNamara84/grobi/releases/latest)**

### 🪟 Windows Executable

Download the standalone executable - no Python installation required!

**Quick Start:**
1. Download `GROBI-vX.X.X-Windows.zip` from the latest release
2. Extract the ZIP file
3. Double-click `GROBI.exe` to start
4. **If Windows SmartScreen appears:**
   - Click "More info"
   - Click "Run anyway"
   - This is a one-time warning

**Requirements:**
- Windows 10 or 11 (64-bit)
- ~25 MB disk space
- Internet connection for DataCite API

### 🍎 macOS Application

Download the native macOS application bundle!

**Quick Start:**
1. Download `GROBI-vX.X.X-macOS.dmg` from the latest release
2. Open the DMG file
3. Drag GROBI.app to your Applications folder
4. **On first launch:**
   - macOS Gatekeeper will block the app (not code-signed)
   - Go to **System Settings** → **Privacy & Security**
   - Click **"Open Anyway"** next to the GROBI warning
   - Or: Right-click GROBI.app → **"Open"** → Confirm
   - This is a one-time process

**Requirements:**
- macOS 10.15 (Catalina) or later
- Intel or Apple Silicon Mac
- ~30 MB disk space
- Internet connection for DataCite API

### ℹ️ About Security Warnings

GROBI is an open-source project and the executables are not code-signed:
- **Windows**: Code signing certificates cost €300-500/year
- **macOS**: Requires Apple Developer Program membership ($99/year)
- The application is built automatically via GitHub Actions
- All source code is publicly available for inspection
- Security warnings are expected and safe to bypass

## Installation from Source

For developers who want to run from source code:

### Requirements

- Python 3.10 or higher
- Windows 11 (recommended for optimal appearance)

### Steps

1. Clone repository:
```bash
git clone https://github.com/McNamara84/grobi.git
cd grobi
```

2. Create and activate virtual environment:
```bash
python -m venv venv
.\venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/macOS
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Development

Install additional dependencies for development:
```bash
pip install -r requirements-dev.txt
```

### Tests

The project includes a comprehensive test suite:

- **287 Unit Tests** for all modules (including UI components and credential management)
- **77% Code Coverage** (Business Logic 90%+)
- **Automated CI/CD** with GitHub Actions

Run tests:
```bash
pytest
```

Run tests with coverage:
```bash
pytest --cov=src --cov-report=html
```

Test coverage by module:
- `datacite_client.py`: 90% - API integration and error handling
- `csv_exporter.py`: 84% - CSV export and validation
- `credentials_dialog.py`: 95% - GUI dialog for credentials
- `main_window.py`: 61% - Main window and threading
- **Overall**: 77%

### Building Windows Executable

To build your own standalone executable:

**Prerequisites:**
- Visual Studio 2022 Build Tools (or MSVC 14.3+)
- All dependencies installed

**Build Steps:**
```bash
# 1. Install build dependencies
pip install -r requirements-build.txt

# 2. Run build script
python scripts/build_exe.py

# 3. Find executable
.\dist\GROBI.exe
```

**Build Output:**
- `dist/GROBI.exe` - Standalone executable (~23 MB)
- `dist/BUILD_INFO.txt` - Build information
- Build time: ~4-5 minutes

**Automated Builds:**

GitHub Actions automatically builds and releases executables when you push a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

See [.github/workflows/README.md](.github/workflows/README.md) for details.

## Usage

Start the application:
```bash
python -m src.main
```

### Workflow 1: Export DOIs to CSV

**Step-by-Step Guide:**

1. **Start application**: Run `python -m src.main` or double-click `GROBI.exe`
2. **Load DOIs**: Click the "📥 DOIs und Landing Page URLs laden" (Load DOIs and landing page URLs) button
3. **Enter credentials**: 
   - Enter your DataCite username (e.g., `TIB.GFZ`)
   - Enter your DataCite password
   - Optional: Enable "Test-API verwenden" (Use Test API) to use the test API instead of production
4. **Retrieve DOIs**: Click "DOIs holen" (Get DOIs)
5. **Wait**: The application displays progress in the status area
6. **Done**: A CSV file is created in the current directory

**CSV Output Format:**
- **Filename**: `{username}_urls.csv` (e.g., `TIB.GFZ_urls.csv`)
- **Encoding**: UTF-8
- **Columns**:
  - `DOI`: The Digital Object Identifier
  - `Landing_Page_URL`: The URL to the dataset's landing page

**Example Output:**
```csv
DOI,Landing_Page_URL
10.5880/GFZ.1.1.2021.001,https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=1234
10.5880/GFZ.1.1.2021.002,https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=5678
```

### Workflow 2: Update Landing Page URLs

**Step-by-Step Guide:**

1. **Prepare CSV file**: Create a CSV file with updated landing page URLs
   - Required format: `DOI,Landing_Page_URL`
   - Must include header row
   - Each DOI must have a corresponding URL
   - URLs must start with `http://` or `https://`

2. **Start update**: Click the "🔄 Landing Page URLs aktualisieren" (Update Landing Page URLs) button

3. **Enter credentials and select file**:
   - Enter your DataCite username
   - Enter your DataCite password
   - Optional: Enable "Test-API verwenden" for testing
   - Click "Durchsuchen..." (Browse) to select your CSV file
   - Button becomes active when credentials and file are provided

4. **Start update process**: Click "Landing Page URLs aktualisieren" (Update Landing Page URLs)

5. **Monitor progress**: 
   - Real-time progress bar shows X/Y DOIs processed
   - Status messages display current operation
   - Errors are logged but process continues

6. **Review results**:
   - Summary dialog shows successful and failed updates
   - Detailed log file created: `update_log_YYYYMMDD_HHMMSS.txt`
   - Log contains all errors with DOI and error message

**CSV Input Format:**
```csv
DOI,Landing_Page_URL
10.5880/GFZ.1.1.2021.001,https://new-url.example.org/dataset1
10.5880/GFZ.1.1.2021.002,https://new-url.example.org/dataset2
```

**Validation Rules:**
- ✅ DOI format: `10.X/...` (where X is 1+ digits, e.g., `10.1/test`, `10.5880/example`)
- ✅ URL format: Must start with `http://` or `https://`
- ✅ All rows must have both DOI and URL
- ❌ Rows with missing DOI are skipped with warning
- ❌ Rows with missing URL cause error
- ℹ️ Final URL validation is performed by DataCite API

**Update Log Example:**
```
======================================================================
GROBI - Landing Page URL Update Log
======================================================================
Datum: 2025-11-19 14:30:00

ZUSAMMENFASSUNG:
  Gesamt: 100 DOIs
  Erfolgreich: 98
  Fehlgeschlagen: 2

======================================================================
FEHLER:
======================================================================
  - 10.5880/GFZ.xxx: DOI nicht gefunden (404)
  - 10.5880/GFZ.yyy: Keine Berechtigung (403)

======================================================================
```

### Workflow 3: Theme Switching

**Change Application Theme:**

The application supports three theme modes accessible via the **Ansicht** (View) menu:

1. **🔄 Auto Mode** (Default):
   - Automatically detects Windows system theme
   - Switches between Light and Dark based on your system settings
   - Menu shows checkmark next to "Auto"

2. **🌙 Dark Mode**:
   - Manually activate dark theme
   - Dark background (#1e1e1e) with light text (#d4d4d4)
   - Optimized for low-light environments
   - Menu shows checkmark next to "Dunkel"

3. **☀️ Light Mode**:
   - Manually activate light theme
   - Light background (#f5f5f5) with dark text (#333)
   - Classic appearance
   - Menu shows checkmark next to "Hell"

**How to Switch:**
1. Click **Ansicht** in the menu bar
2. Select **Theme** from the dropdown
3. Choose **Auto**, **Hell** (Light), or **Dunkel** (Dark)
4. Theme preference is saved and restored on next launch

**Additional Menu Options:**

The **Hilfe** (Help) menu provides quick access to:
- **Über GROBI...**: Opens About dialog with version info, logo, and links
- **Changelog anzeigen**: Opens CHANGELOG.md or GitHub releases page
- **GitHub-Repository öffnen**: Opens project repository in browser

**System Theme Detection:**
- Windows 10/11: Automatically detects system-wide dark mode setting
- Settings → Personalization → Colors → "Choose your color"

### Workflow 4: Export DOIs with Authors to CSV

**Step-by-Step Guide:**

1. **Start application**: Run `python -m src.main` or double-click `GROBI.exe`
2. **Load Authors**: Click the "👥 DOIs und Autoren laden" (Load DOIs and authors) button
3. **Enter credentials**: 
   - Enter your DataCite username (e.g., `TIB.GFZ`)
   - Enter your DataCite password
   - Optional: Enable "Test-API verwenden" (Use Test API) to use the test API instead of production
4. **Retrieve DOIs**: Click "DOIs holen" (Get DOIs)
5. **Wait**: The application displays progress in the status area
6. **Done**: A CSV file with creator information is created in the current directory

**CSV Output Format:**
- **Filename**: `{username}_authors.csv` (e.g., `TIB.GFZ_authors.csv`)
- **Encoding**: UTF-8
- **Columns**:
  - `DOI`: The Digital Object Identifier
  - `Creator Name`: Full name of the creator
  - `Name Type`: Either "Personal" or "Organizational"
  - `Given Name`: First name (empty for organizations)
  - `Family Name`: Last name (empty for organizations)
  - `Name Identifier`: ORCID URL if available
  - `Name Identifier Scheme`: "ORCID" if available
  - `Scheme URI`: ORCID scheme URI if available

**Example Output:**
```csv
DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI
10.5880/GFZ.1.1.2021.001,Miller, Elizabeth,Personal,Elizabeth,Miller,https://orcid.org/0000-0001-5000-0007,ORCID,https://orcid.org
10.5880/GFZ.1.1.2021.001,Smith, John,Personal,John,Smith,,,
10.5880/GFZ.1.1.2021.002,GFZ Data Services,Organizational,,,,,
```

**Special Cases:**
- **Multiple Creators**: Each creator appears as a separate row, so a DOI may appear multiple times
- **Organizations**: `Given Name` and `Family Name` fields are empty
- **No ORCID**: `Name Identifier`, `Name Identifier Scheme`, and `Scheme URI` fields are empty
- **Other Identifiers**: Only ORCID identifiers are exported; other identifier schemes (e.g., ResearcherID) are ignored
- **No Creators**: DOIs without creators are skipped with a warning in the log

### Workflow 5: Update Authors/Creators

**Step-by-Step Guide:**

1. **Prepare CSV file**: Create a CSV file with updated creator information
   - **Required format**: 8 columns with exact header names (see below)
   - Must include header row with spaces (not underscores)
   - Each creator appears as a separate row
   - DOI can appear multiple times (one row per creator)
   - Order of rows determines creator order in DataCite

2. **Start update**: Click the "🖊️ Autoren aktualisieren" (Update Authors) button

3. **Enter credentials and select file**:
   - Enter your DataCite username
   - Enter your DataCite password
   - Optional: Enable "Test-API verwenden" for testing
   - Click "Durchsuchen..." (Browse) to select your CSV file
   - Button becomes active when credentials and file are provided

4. **Dry Run Validation**:
   - Click "Autoren aktualisieren" (Update Authors)
   - Application performs **automatic validation** (dry run):
     - Fetches current metadata for each DOI
     - Checks if creator count matches between CSV and DataCite
     - Validates CSV format and data integrity
   - Review validation results in dialog:
     - ✅ Valid: Shows creator count, ready for update
     - ❌ Invalid: Shows specific error (DOI not found, count mismatch, etc.)

5. **Confirm and Update**:
   - Review dry run results carefully
   - Click "Weiter mit Update" (Continue with Update) to proceed
   - Or click "Abbrechen" (Cancel) to abort
   - Only validated DOIs will be updated

6. **Monitor progress**:
   - Real-time progress bar shows X/Y DOIs processed
   - Status messages display current operation
   - Each DOI update is logged individually

7. **Review results**:
   - Summary dialog shows successful and failed updates
   - Validation results from dry run included
   - Application log contains detailed information

**CSV Input Format:**

The CSV must have these **exact column headers** (with spaces, not underscores):

```csv
DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI
10.5880/GFZ.1.1.2021.001,"Smith, John",Personal,John,Smith,0000-0001-5000-0007,ORCID,https://orcid.org
10.5880/GFZ.1.1.2021.001,"Doe, Jane",Personal,Jane,Doe,,,
10.5880/GFZ.1.1.2021.002,GFZ Data Services,Organizational,,,,,,
```

**Important:** Use the exported CSV from "👥 DOIs und Autoren laden" as a template!

**Validation Rules:**

**DOI Format:**
- ✅ Must match pattern: `10.X/...` (where X is 1+ digits)
- ❌ Invalid: `10.5880` (missing suffix), `DOI:10.5880/test` (prefix not allowed)

**Creator Name:**
- ✅ Required for all creators
- ✅ Use quotes for names with commas: `"Smith, John"`
- ❌ Empty creator name causes error

**Name Type:**
- ✅ Must be either "Personal" or "Organizational"
- ✅ If empty, defaults to "Personal"
- ❌ Invalid values (e.g., "Person") cause error

**Given/Family Name:**
- ✅ Personal: Can be empty (flexible for single names)
- ✅ Organizational: Must be empty
- ❌ Organizational with Given/Family Name causes error

**ORCID (Name Identifier):**
- ✅ Must start with `0000-` if provided
- ✅ Full format: `0000-XXXX-XXXX-XXX[X or digit]`
- ✅ Invalid ORCID generates warning (non-fatal)
- ✅ Can be empty
- ℹ️ Example valid ORCIDs:
  - `0000-0001-5000-0007`
  - `0000-0002-1825-0097`
- ❌ Example invalid: `1234-5678-9012-3456` (must start with 0000)

**Name Identifier Scheme / Scheme URI:**
- ✅ Only used when Name Identifier is provided
- ✅ Typical values: `ORCID` and `https://orcid.org`
- ✅ Can be empty if no identifier

**Row Order:**
- ⚠️ **CRITICAL**: Row order in CSV determines creator order in DataCite
- First row for a DOI = First creator (primary author)
- Maintain proper order when editing CSV

**Dry Run Validation:**
- ✅ Automatic validation before any updates
- ✅ Checks creator count match (CSV vs. DataCite)
- ✅ Verifies DOI exists and is accessible
- ✅ Only validated DOIs proceed to update
- ℹ️ Validation errors prevent updates to affected DOIs

**Examples:**

**Valid Personal Creator with ORCID:**
```csv
10.5880/GFZ.1.1.2021.001,"Miller, Elizabeth",Personal,Elizabeth,Miller,0000-0001-5000-0007,ORCID,https://orcid.org
```

**Valid Personal Creator without ORCID:**
```csv
10.5880/GFZ.1.1.2021.002,"Smith, John",Personal,John,Smith,,,
```

**Valid Organizational Creator:**
```csv
10.5880/GFZ.1.1.2021.003,GFZ Data Services,Organizational,,,,,,
```

**Multiple Creators for Same DOI:**
```csv
10.5880/GFZ.1.1.2021.004,"Doe, Jane",Personal,Jane,Doe,,,
10.5880/GFZ.1.1.2021.004,"Smith, Bob",Personal,Bob,Smith,0000-0002-1234-5678,ORCID,https://orcid.org
10.5880/GFZ.1.1.2021.004,Example Institute,Organizational,,,,,,
```

### Workflow 6: Credential Management

**Managing Multiple DataCite Accounts:**

The application securely stores your DataCite credentials using Windows Credential Manager, allowing you to save and manage multiple accounts (e.g., production and test environments).

**Saving Credentials:**

1. **First-time use**: When you successfully authenticate with new credentials, the application automatically offers to save them:
   - A dialog appears after your first successful API call
   - Enter a descriptive name for the account (e.g., "GFZ Production" or "Test Account")
   - Click "Speichern" to store the credentials securely
   - Click "Nicht speichern" to skip (you'll be asked again next time)

2. **Automatic prompt**: The save dialog appears once per workflow after the first successful operation
   - DOI fetch: After successful retrieval
   - URL update: After first successful DOI update
   - Authors update: After first successful metadata update

**Using Saved Credentials:**

1. **Account selection**: When opening a credentials dialog, you'll see a dropdown with your saved accounts:
   - Format: `Account Name (username - API-Type)`
   - Example: `GFZ Production (TIB.GFZ - Produktiv-API)`
   - Select an account to automatically fill in the credentials

2. **Last used account**: The application automatically selects your most recently used account

3. **New credentials**: Select "➕ Neue Zugangsdaten eingeben" to enter different credentials

**Managing Accounts:**

1. **Delete account**: 
   - Select the account you want to remove from the dropdown
   - Click the 🗑️ delete button
   - Confirm the deletion
   - Both the account metadata and the password are securely removed

2. **Security**: 
   - Passwords are stored in Windows Credential Manager (encrypted by the OS)
   - Account metadata is stored locally in `%APPDATA%/GROBI/credentials_metadata.json`
   - Only your Windows user account can access the stored passwords

**Security Notes:**

- Credentials are stored per Windows user account
- Passwords are never stored in plain text
- The application never logs or displays stored passwords
- Deleting an account removes all associated data
- You can manage stored passwords via Windows Credential Manager:
  - Open "Credential Manager" in Windows Settings
  - Look for entries starting with "GROBI_DataCite:"

### Workflow 7: Database Synchronization (Optional)

**⚠️ GFZ-Internal Feature - Requires VPN Connection**

GROBI can automatically synchronize author metadata updates with the internal GFZ database (SUMARIOPMD) in addition to DataCite. This ensures data consistency across both systems.

**Requirements:**
- Active VPN connection to GFZ network
- Database credentials (contact GFZ Data Services)
- Write permissions for SUMARIOPMD database

**Setup:**

1. **Configure Database Connection:**
   - Open Settings: Menu → Einstellungen (or press Ctrl+,)
   - Switch to "Datenbank" tab
   - Enter database credentials:
     - Host: `rz-mysql3.gfz-potsdam.de`
     - Database: `sumario-pmd`
     - Username: Your database username
     - Password: Your database password
   - Click "Verbindung testen" (Test Connection)
   - Wait for status: ✓ Verbunden (Connected) or ✗ Fehler (Error)
   - If successful, check "☑ Datenbank-Updates aktivieren"
   - Click "Speichern" (Save)

2. **Credentials are stored securely:**
   - Password encrypted via Windows Credential Manager
   - Same security as DataCite credentials
   - Can be deleted anytime via Settings dialog

**How Database Sync Works:**

When database synchronization is enabled, author metadata updates follow a **Database-First Two-Phase-Commit** pattern:

```
Phase 1: Validation
  ├─ ✓ DataCite API reachable?
  └─ ✓ Database reachable?
  
Phase 2: Execution (Database-First!)
  ├─ 1. Database Update (with ROLLBACK capability)
  │  ├─ START TRANSACTION
  │  ├─ UPDATE resourceagent table
  │  └─ Success → COMMIT, proceed to DataCite
  │     Error → ROLLBACK, ABORT (nothing committed!)
  │
  └─ 2. DataCite Update (only if database succeeded!)
     ├─ Success → ✓ Both systems synchronized
     ├─ Error → Immediate Retry (1-2 attempts)
     └─ Retry failed → Logged as inconsistency
```

**Why Database-First?**
- ✅ Database has real ROLLBACK (SQL transaction)
- ✅ DataCite has NO rollback (once pushed = permanent)
- ✅ Database errors more likely (VPN drops, locks)
- ✅ Minimizes inconsistency risk from ~50% to ~5%

**Update Process:**

1. **Start Author Update** (Workflow 5)
2. **Automatic Validation:**
   - Application checks DataCite API availability
   - If database sync enabled: checks database connection
   - **Both must be available** to proceed
   - Error example: "Datenbank-Updates aktiviert, aber DB nicht erreichbar. Bitte VPN-Verbindung prüfen..."

3. **For Each DOI:**
   - Progress shows separate status for each system:
     ```
     DOI 2/5: 10.5880/gfz.example.001
       ✓ Validierung erfolgreich
       → Datenbank aktualisieren...
       ✓ Datenbank erfolgreich
       → DataCite aktualisieren...
       ✓ DataCite erfolgreich
     ```

4. **Update Log File:**
   - Timestamped file: `update_log_YYYYMMDD_HHMMSS.txt`
   - Shows database sync status
   - Lists any inconsistencies (database OK, DataCite failed)
   - Example:
     ```
     ======================================================================
     DATABASE-FIRST UPDATE PATTERN:
       1. Validation Phase: Beide Systeme erreichbar? ✓
       2. Database Update: ZUERST aktualisiert (mit ROLLBACK)
       3. DataCite Update: DANACH aktualisiert (mit Retry bei Fehler)
     
     Dieses Pattern minimiert Inkonsistenzen:
       - DB-Fehler → ROLLBACK (nichts committed)
       - DataCite-Fehler → Sofortiger Retry (1-2 Versuche)
       - Beide Fehler → Update abgebrochen
     ======================================================================
     
     ZUSAMMENFASSUNG:
       Gesamt: 10 DOIs
       Erfolgreich: 9
       Fehlgeschlagen: 1
       DB-Sync Status: Aktiviert ✓
       Kritische Inkonsistenzen: 0 (manuelle Korrektur erforderlich)
     ```

**Handling Inconsistencies:**

In rare cases (~5%), database update succeeds but DataCite fails even after retry:

- ⚠️ **Database committed, DataCite not updated** = Inconsistency
- Log file shows: `✓ Datenbank erfolgreich` but `✗ DataCite fehlgeschlagen (DB bereits committed!)`
- **Action required:** Manual correction in DataCite
- Recommendation: Use CSV export to verify current DataCite state, then re-run update

**Disabling Database Sync:**

1. Open Settings → Datenbank tab
2. Uncheck "☑ Datenbank-Updates aktivieren"
3. Click "Speichern"
4. Author updates will only affect DataCite (classic behavior)
5. VPN connection no longer required

**Notes:**

- Database sync is **optional** - GROBI works fine without it
- Only affects author metadata updates (Workflow 5)
- Does NOT affect Landing Page URL updates (Workflow 2)
- VPN connection required only when database sync is enabled
- Connection tested before each update batch (validation phase)
- Database credentials stored as securely as DataCite credentials
- Only updates **Creators** in database, never Contributors

### Notes:

- The application retrieves **all** DOIs registered with the specified username
- For many DOIs, retrieval may take several seconds
- Progress indicator shows real-time status
- Error messages are displayed in German
- CSV files are automatically overwritten if they already exist
- CSV filenames: `{username}_urls.csv` for URL export, `{username}_authors.csv` for author export
- Update process continues even if individual DOIs fail
- Each update creates a timestamped log file for auditing
- Theme preference persists across application restarts
- Author export: One row per creator (DOIs with multiple creators appear multiple times)
- **Author updates: Automatic dry run validation prevents errors before making changes**
- Creator order in CSV determines order in DataCite (first row = primary author)
- **Credential management: Securely store and manage multiple DataCite accounts**

## Project Structure

```
grobi/
├── src/
│   ├── __version__.py               # Version and metadata
│   ├── main.py                      # Entry point
│   ├── ui/                          # GUI components
│   │   ├── main_window.py          # Main window with menubar and workflow groups
│   │   ├── about_dialog.py         # About dialog with version info and links
│   │   ├── credentials_dialog.py   # Credentials dialog with account management
│   │   ├── save_credentials_dialog.py # Post-authentication save dialog
│   │   └── theme_manager.py        # Theme management (AUTO/LIGHT/DARK)
│   ├── api/                         # DataCite API client
│   │   └── datacite_client.py      # API methods (fetch, update metadata/URLs)
│   ├── workers/                     # Background workers
│   │   ├── update_worker.py        # URL update worker with threading
│   │   └── authors_update_worker.py # Creator update worker with dry run
│   └── utils/                       # Utility functions
│       ├── csv_exporter.py         # CSV export functionality
│       ├── csv_parser.py           # CSV parsing and validation
│       └── credential_manager.py   # Secure credential storage (Windows Credential Manager)
├── tests/                           # Unit tests (287 tests, 77% coverage)
│   ├── test_csv_parser.py          # CSV parsing tests (26 tests)
│   ├── test_datacite_client.py     # API fetch tests
│   ├── test_datacite_client_creators.py  # API creator fetch tests
│   ├── test_datacite_client_update.py  # API update tests
│   ├── test_datacite_client_authors_update.py  # API creator update tests (17 tests)
│   ├── test_csv_exporter.py        # CSV export tests
│   ├── test_update_worker.py       # URL worker tests
│   ├── test_authors_update_worker.py # Creator worker tests (14 tests)
│   ├── test_theme_manager.py       # Theme management tests
│   ├── test_about_dialog.py        # About dialog tests (14 tests)
│   ├── test_main_window.py         # Main window tests including UI components (30 tests)
│   ├── test_credential_manager.py  # Credential manager tests (28 tests)
│   ├── test_credentials_dialog.py  # Credentials dialog tests (45 tests)
│   ├── test_save_credentials_dialog.py # Save credentials dialog tests (17 tests)
│   └── test_credential_save_integration.py # Integration tests (13 tests)
├── requirements.txt                 # Production dependencies
├── requirements-dev.txt             # Development dependencies
└── requirements-build.txt           # Build dependencies (Nuitka)
```

## DataCite API

The application uses the DataCite REST API v2:
- Production: https://api.datacite.org
- Test: https://api.test.datacite.org

More information: [DataCite API Documentation](https://support.datacite.org/docs/api)

## Error Handling

The application handles the following error scenarios:

- **Authentication errors**: Invalid credentials
- **Network errors**: No internet connection
- **Timeout**: API not responding in time
- **Rate limiting**: Too many requests to the API
- **CSV errors**: No write permissions or insufficient disk space
- **No DOIs**: User has no registered DOIs
- **CSV Validation Errors**: Invalid DOI format (must be `10.XXXX/...`) or invalid URL format (must be `http://` or `https://`)
- **DOI Not Found (404)**: DOI does not exist in DataCite registry
- **No Permission (403)**: User does not have permission to update the DOI

All errors are displayed in a user-friendly manner. For bulk update operations, the application **continues processing even if individual DOIs fail**, logging all errors to a timestamped log file (`update_log_YYYYMMDD_HHMMSS.txt`).

## Logging

The application creates log files in the application directory:

### Application Log (`grobi.log`)
Contains detailed information about:
- Started operations
- API calls and responses
- Errors and exceptions
- CSV export operations

### Update Log Files (`update_log_YYYYMMDD_HHMMSS.txt`)
Created for each bulk update operation with:
- Timestamp and operation summary
- Success and error counts
- Detailed list of all failed DOIs with error messages
- Complete audit trail for troubleshooting

Example:
```
Update Log - 2025-01-27 14:30:45
================================
Total DOIs processed: 10
Successful updates: 8
Failed updates: 2

Failed DOI Details:
- DOI: 10.5555/example1 | Error: DOI not found (404)
- DOI: 10.5555/example2 | Error: No permission to update DOI (403)
```

## Technical Details

- **Qt6-based GUI** with modern Windows 11 design
- **Asynchronous API calls** without blocking the user interface
- **HTTP Basic Authentication** for DataCite API
- **Secure credential storage** with Windows Credential Manager integration (keyring library)
- **Automatic pagination** for large datasets
- **UTF-8 encoding** for international characters
- **Thread-safe implementation** with Qt Signals/Slots
- **CSV validation** with regex patterns:
  - DOI format: `10\.\d{4,}/\S+`
  - URL format: `http(s)://...`
- **Background threading** for long-running bulk update operations
- **Real-time progress tracking** with X/Y counter display
- **Continue-on-error strategy** for bulk operations with comprehensive logging
- **Multi-account management** with account metadata stored in `%APPDATA%/GROBI/`

## License

See [LICENSE](LICENSE) file.

## Author

Holger Ehrmann, GFZ Data Services, GFZ Helmholtz Centre for Geosciences

## Built With

- Python 3.10+ (Developed and tested with Python 3.13)
  - **Note:** Release executables are built with Python 3.12 for better MSVC compatibility and stable Nuitka support
- PySide6 (Qt6) - Modern GUI framework
- keyring >=24.3.0 - Secure credential storage with Windows Credential Manager
- DataCite REST API v2 - DOI metadata management

