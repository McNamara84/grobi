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
- 🔄 **Update Landing Page URLs**: Bulk update landing page URLs via CSV import
- 📝 **Detailed Logging**: Automatic creation of update logs with success/error reports

### Technical Features
- 🧪 Support for test and production API
- 🎨 Modern user interface with PySide6 (Qt6)
- 🌓 **Dark Mode Support**: Auto-detection of system theme with manual override (AUTO/LIGHT/DARK)
- ⚡ Non-blocking API calls with progress indication
- 📈 Real-time progress tracking for bulk operations
- 🛡️ Comprehensive error handling and validation
- 📋 CSV format validation (DOI and URL format checks)
- 💾 Persistent theme preference across sessions

## Download

### Windows Executable (Recommended for End Users)

Download the latest standalone executable - no Python installation required!

**[📥 Download Latest Release](https://github.com/McNamara84/grobi/releases/latest)**

**Quick Start:**
1. Download `GROBI-vX.X.X-Windows.zip` from the latest release
2. Extract the ZIP file
3. Double-click `GROBI.exe` to start
4. **If Windows SmartScreen appears:**
   - Click "More info"
   - Click "Run anyway"
   - This is a one-time warning

**About SmartScreen Warning:**
GROBI is an open-source project and the executable is not code-signed (commercial certificates cost €300-500/year). Windows SmartScreen shows a warning for unsigned executables. This is expected and safe - the application is built automatically via GitHub Actions and the source code is publicly available for inspection.

### Requirements for Standalone EXE

- Windows 10 or 11 (64-bit)
- ~25 MB disk space
- Internet connection for DataCite API

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

- **63 Unit Tests** for all modules
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
- **Filename**: `{username}.csv` (e.g., `TIB.GFZ.csv`)
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

The application supports three theme modes:

1. **🔄 Auto Mode** (Default):
   - Automatically detects Windows system theme
   - Switches between Light and Dark based on your system settings
   - Button shows: `🔄 Auto (Light)` or `🔄 Auto (Dark)`

2. **🌙 Dark Mode**:
   - Manually activate dark theme
   - Dark background (#1e1e1e) with light text (#d4d4d4)
   - Optimized for low-light environments
   - Button shows: `☀️ Light Mode` (to switch back)

3. **☀️ Light Mode**:
   - Manually activate light theme
   - Light background (#f5f5f5) with dark text (#333)
   - Classic appearance
   - Button shows: `🌙 Dark Mode` (to switch)

**How to Switch:**
- Click the theme button in the main window
- Cycles through: AUTO → LIGHT → DARK → AUTO
- Theme preference is saved and restored on next launch

**System Theme Detection:**
- Windows 10/11: Automatically detects system-wide dark mode setting
- Settings → Personalization → Colors → "Choose your color"

### Notes:

- The application retrieves **all** DOIs registered with the specified username
- For many DOIs, retrieval may take several seconds
- Progress indicator shows real-time status
- Error messages are displayed in German
- CSV files are automatically overwritten if they already exist
- Update process continues even if individual DOIs fail
- Each update creates a timestamped log file for auditing
- Theme preference persists across application restarts

## Project Structure

```
grobi/
├── src/
│   ├── main.py                      # Entry point
│   ├── ui/                          # GUI components
│   │   ├── main_window.py          # Main window with DOI export and URL update
│   │   ├── credentials_dialog.py   # Credentials dialog (dual mode)
│   │   └── theme_manager.py        # Theme management (AUTO/LIGHT/DARK)
│   ├── api/                         # DataCite API client
│   │   └── datacite_client.py      # API methods (fetch, update)
│   ├── workers/                     # Background workers
│   │   └── update_worker.py        # URL update worker with threading
│   └── utils/                       # Utility functions
│       ├── csv_exporter.py         # CSV export functionality
│       └── csv_parser.py           # CSV parsing and validation
├── tests/                           # Unit tests (121 tests, 77% coverage)
│   ├── test_csv_parser.py          # CSV parsing tests
│   ├── test_datacite_client_update.py  # API update tests
│   ├── test_update_worker.py       # Worker tests
│   └── test_theme_manager.py       # Theme management tests
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
- **Automatic pagination** for large datasets
- **UTF-8 encoding** for international characters
- **Thread-safe implementation** with Qt Signals/Slots
- **CSV validation** with regex patterns:
  - DOI format: `10\.\d{4,}/\S+`
  - URL format: `http(s)://...`
- **Background threading** for long-running bulk update operations
- **Real-time progress tracking** with X/Y counter display
- **Continue-on-error strategy** for bulk operations with comprehensive logging

## License

See [LICENSE](LICENSE) file.

## Author

Holger Ehrmann, GFZ Data Services, GFZ Helmholtz Centre for Geosciences

## Built With

- Python 3.10+ (Developed and tested with Python 3.13)
  - **Note:** Release executables are built with Python 3.12 for better MSVC compatibility and stable Nuitka support
- PySide6 (Qt6)
- DataCite REST API v2

