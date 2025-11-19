# GROBI

![Tests](https://github.com/McNamara84/grobi/workflows/Tests/badge.svg)
![Coverage](coverage.svg)
![Python](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)
![License](https://img.shields.io/badge/license-MIT-green)

**G**FZ **R**esearch Data Repository **O**perations & **B**atch **I**nterface

A modern GUI tool for GFZ Data Services to manage DataCite DOIs.

## Features

- 🔍 Retrieve all registered DOIs from DataCite API
- 📊 Export as CSV file (DOI + Landing Page URL)
- 🧪 Support for test and production API
- 🎨 Modern user interface with PySide6 (Qt6)
- ⚡ Non-blocking API calls with progress indication
- 🛡️ Comprehensive error handling

## Requirements

- Python 3.10 or higher
- Windows 11 (recommended for optimal appearance)

## Installation

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

## Usage

Start the application:
```bash
python -m src.main
```

### Step-by-Step Guide:

1. **Start application**: Run `python -m src.main`
2. **Load DOIs**: Click the "📥 DOIs laden" (Load DOIs) button
3. **Enter credentials**: 
   - Enter your DataCite username (e.g., `TIB.GFZ`)
   - Enter your DataCite password
   - Optional: Enable "Test-API verwenden" (Use Test API) to use the test API instead of production
4. **Retrieve DOIs**: Click "DOIs holen" (Get DOIs)
5. **Wait**: The application displays progress in the status area
6. **Done**: A CSV file is created in the current directory

### CSV File Format:

The exported CSV file contains:
- **Filename**: `{username}.csv` (e.g., `TIB.GFZ.csv`)
- **Encoding**: UTF-8
- **Columns**:
  - `DOI`: The Digital Object Identifier
  - `Landing_Page_URL`: The URL to the dataset's landing page

Example:
```csv
DOI,Landing_Page_URL
10.5880/GFZ.1.1.2021.001,https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=1234
10.5880/GFZ.1.1.2021.002,https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=5678
```

### Notes:

- The application retrieves **all** DOIs registered with the specified username
- For many DOIs, retrieval may take several seconds
- Progress indicator shows real-time status
- Error messages are displayed in German
- CSV file is automatically overwritten if it already exists

## Project Structure

```
grobi/
├── src/
│   ├── main.py              # Entry point
│   ├── ui/                  # GUI components
│   ├── api/                 # DataCite API client
│   └── utils/               # Utility functions
├── tests/                   # Unit tests
├── requirements.txt         # Production dependencies
└── requirements-dev.txt     # Development dependencies
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

All errors are displayed in a user-friendly manner.

## Logging

The application creates a `grobi.log` file in the application directory with detailed information about:
- Started operations
- API calls and responses
- Errors and exceptions
- CSV export operations

## Technical Details

- **Qt6-based GUI** with modern Windows 11 design
- **Asynchronous API calls** without blocking the user interface
- **HTTP Basic Authentication** for DataCite API
- **Automatic pagination** for large datasets
- **UTF-8 encoding** for international characters
- **Thread-safe implementation** with Qt Signals/Slots

## License

See [LICENSE](LICENSE) file.

## Author

Holger Ehrmann, GFZ Data Services, GFZ Helmholtz Centre for Geosciences

## Built With

- Python 3.10+ (Developed and tested with Python 3.13)
- PySide6 (Qt6)
- DataCite REST API v2

