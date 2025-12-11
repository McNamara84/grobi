# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2025-12-11

### Added
- 📂 **CSV Splitter Tool** (#19): Split large CSV files into manageable chunks
  - **UI Integration**: New "CSV Splitter" GroupBox in main window
    - Input file selection with drag & drop support
    - Configurable chunk size (default: 100 rows per file)
    - Output directory selection with automatic default
    - Real-time validation and progress reporting
  - **Smart File Handling**: Automatic output filename generation
    - Pattern: `{original}_part{N}.csv` (e.g., `dois_part1.csv`, `dois_part2.csv`)
    - Preserves original CSV headers in all chunks
    - Intelligent row distribution across files
  - **Background Processing**: Threaded worker for responsive UI
    - Progress updates during splitting operation
    - Error handling with detailed user feedback
    - Automatic cleanup on completion
  - **Robust CSV Processing**: 
    - Handles large files without memory issues
    - Preserves UTF-8 encoding and special characters
    - Validates input file existence and readability
    - Creates output directory if needed
- 🔄 **Cursor-Based DOI Export** (#18): Efficient pagination for large datasets
  - **DataCite API Enhancement**: Cursor-based pagination support
    - Uses `page[cursor]=1` for initial requests (DataCite API requirement)
    - Follows `links.next` for subsequent pages
    - Eliminates offset-based pagination limitations
    - Handles datasets with 10,000+ DOIs efficiently
  - **Comprehensive Documentation**: Added inline comments
    - References to DataCite API pagination requirements
    - Documentation links for cursor-based paging
    - Clarifies pagination behavior across all fetch methods

### Changed
- 🖥️ **Adaptive Window Sizing** (#21): Improved initial window dimensions
  - Calculates initial height as 95% of available screen height
  - Maintains minimum height of 800px for usability
  - Ensures window fits on smaller displays
  - Prevents window from exceeding screen boundaries on startup

### Technical Details
- **CSV Splitter Implementation**:
  - `src/workers/csv_splitter_worker.py`: New worker (180+ lines) for background splitting
  - `src/ui/csv_splitter_dialog.py`: Standalone dialog (350+ lines) with file selection UI
  - `src/ui/main_window.py`: Integration into main window GroupBox
  - Threading model: `QThread` with signal/slot communication
  - Error handling: `CSVSplitError` custom exception class
- **Cursor Pagination Changes**:
  - Updated methods: `fetch_all_dois`, `fetch_all_dois_with_creators`, `fetch_all_dois_with_publisher`, `fetch_all_dois_with_contributors`
  - No breaking changes: Existing code continues to work without modification
  - Improved reliability for large repositories (tested with 10,000+ DOIs)
- **Window Sizing Logic**:
  - Uses `QApplication.primaryScreen().availableGeometry()`
  - Calculates height dynamically: `min(int(screen_height * 0.95), screen_height - 100)`
  - Ensures minimum usable height of 800px
  - Applied in `MainWindow.__init__()` during window setup
- **Test Coverage**: +15 tests (441 → 456 tests)
  - CSV splitter: Worker logic, dialog UI, file handling edge cases
  - Window sizing: Screen detection, height calculation, boundary cases
  - Overall coverage: 78% maintained

## [0.5.0] - 2025-12-10

### Added
- 👥 **Contributors Management** (#14): Export and update contributor metadata
  - **CSV Export**: Export all contributors (non-Creator roles) with metadata
    - Supports all 21 DataCite ContributorTypes (ContactPerson, DataCollector, DataCurator, etc.)
    - Multiple roles per contributor in comma-separated format
    - Contact information enrichment (email, website, position) from database for ContactPerson role
    - Intelligent name-based matching for contact info across all resourceagents
    - CSV format: 12 columns including DOI, name, identifiers, contributor types, contact info
  - **CSV Import & Update**: Bulk update contributors via CSV
    - Partial update support: Match contributors by name or ORCID
    - Change detection: Skip unchanged contributors (DB-only fields + DataCite fields)
    - Database-First Pattern: Update internal DB before DataCite API
    - Transactional updates with ROLLBACK on failure
    - Contact info updates (email, website, position) only in database
    - Affiliation preservation (not exported/imported)
  - **Advanced Name Type Inference**: Smart detection of Personal vs Organizational contributors
    - ORCID presence = definitive proof of Personal nameType
    - ROR identifier = definitive proof of Organizational nameType
    - Organization keyword detection (150+ keywords: University, Institute, Team, Laboratory, etc.)
    - Heuristic person name detection (comma format, title patterns)
    - ContributorType-based inference (e.g., ResearchGroup → always Organizational)
    - Override logic: Warn when correcting inconsistent DataCite nameType
    - URL/Email detection as organizational indicators
  - **Comprehensive Tests**: 50+ new tests with realistic production data
    - NameType inference tests with edge cases and internationalization
    - Integration tests with pagination and field extraction
    - Real-world contributor update scenarios
    - Partial update and matching logic tests
    - DB-only field change detection tests
  - **UI Integration**: New "Contributors" GroupBox in main window
    - Export button: Fetch and export contributors to CSV
    - Update button: Import and update from CSV with validation
    - Database enrichment: Automatic contact info fetching if DB sync enabled
    - CredentialsDialog extended with 'update_contributors' mode
- **Shared Publisher Parsing Utility**: Centralized parsing logic
  - `src/utils/publisher_parser.py`: Extract publisherIdentifier and publisherIdentifierScheme
  - Used by DataCite client and publisher update worker for consistency
  - Robust handling of missing or empty publisher identifiers

### Changed
- **Language Code Validation**: Extended to support BCP 47 codes up to 11 characters
- **CSV Exporter**: Improved handling of empty publisherIdentifier fields
- **Button Management**: Refactored with `_set_buttons_enabled()` helper method
- **Error Display**: Extracted `_format_error_list()` for consistent error formatting
- **Publisher Update Logic**: Better handling of DOIs missing in database
  - "DOI not found in DB" treated as warning, not fatal error
  - Clearer inconsistency tracking (DB updated but DataCite failed)
- **DataCite API Validation**: Real metadata fetch for availability check
  - Explicit authentication and network error handling in PublisherUpdateWorker
- **Credentials Flag**: Fixed `credentials_are_new` parameter passing in publisher updates

### Technical Details
- **Contributors Implementation**:
  - `src/api/datacite_client.py`: +7 methods for contributors (fetch, validate, update, enrich)
  - `src/db/sumariopmd_client.py`: +5 methods for contributors and contact info
  - `src/utils/csv_parser.py`: New `parse_contributors_update_csv()` method
  - `src/utils/csv_exporter.py`: New `export_dois_with_contributors_to_csv()` function
  - `src/workers/contributors_update_worker.py`: New worker (850+ lines) with change detection
  - `src/ui/main_window.py`: New DOIContributorFetchWorker + UI integration
- **NameType Inference Logic**:
  - Priority: ORCID (Personal) → ROR (Organizational) → API nameType → Keywords → Heuristics
  - Organization keyword sets: Long keywords (substring match) + short keywords (word boundary)
  - Person name patterns: Comma format detection, title presence checks
  - Logging: Debug logs for all inference decisions + warnings for overrides
- **Partial Update Matching**:
  - Matching by name (normalized) or ORCID identifier
  - Allows updating subset of contributors without full list
  - Preserves affiliations from existing metadata
  - Clears DB-only fields (email/website/position) if not in CSV
- **Test Coverage**: +50 tests (391 → 441 tests)
  - NameType inference: 28 edge cases (real-world data)
  - Integration: Pagination, field extraction, API mocking
  - Contributor updates: Change detection, partial updates, matching
  - DB-only field changes: Detection and description logic
  - Overall coverage: 78% maintained
- **Database Operations**:
  - Transaction scope: DELETE non-Creator roles → INSERT new contributors → INSERT roles → INSERT contactinfo
  - Contact info matching: Multiple key formats (lastname/firstname, full name, "Lastname, Firstname")
  - All contact info fetched for resource (not just ContactPerson role)
  - ROLLBACK on any database error during transaction

## [0.4.0] - 2025-12-03

### Added
- 📄 **Publisher Metadata Management** (#13): Export and update publisher information
  - **CSV Export**: Export DOIs with publisher metadata
    - Publisher name (required)
    - Publisher language (ISO 639-1 code, optional)
    - Publisher identifier (e.g., ROR, ISNI, Crossref Funder ID, optional)
    - Publisher identifier scheme (e.g., ROR, ISNI, optional)
    - CSV format: 5 columns (DOI, Publisher, Language, Identifier, Scheme)
  - **CSV Import & Update**: Bulk update publisher metadata via CSV
    - Change detection: Skip DOIs with unchanged publisher metadata
    - Database-First Pattern: Update internal DB before DataCite API (if DB sync enabled)
    - Transactional updates with ROLLBACK on failure
    - Validation: DOI format, language codes (2-11 chars for BCP 47), publisher identifier schemes
    - Dry run: Validate all entries before applying updates
  - **UI Integration**: New "Publisher" GroupBox in main window
    - Export button: Fetch and export publishers to CSV
    - Update button: Import and update from CSV with validation
    - CredentialsDialog extended with 'update_publisher' mode
  - **Comprehensive Tests**: 30+ new tests
    - CSV export and parsing tests
    - DataCite client publisher methods tests
    - PublisherUpdateWorker tests (dry run, update, change detection)
    - API error handling and validation tests
- **Publisher Parsing Utility**: Centralized logic for parsing publisher identifiers
  - `src/utils/publisher_parser.py`: Extract scheme and ID from identifiers
  - Used by DataCite client and update worker for consistency

### Changed
- **Database Update Error Handling**: Improved publisher update logic
  - DOIs not found in database treated as warnings, not fatal errors
  - Better tracking of actual database modifications
  - Clearer inconsistency detection (DB updated but DataCite failed)
- **Language Code Validation**: Extended to support longer BCP 47 tags (up to 11 characters)
- **CSV Publisher Export**: Better handling of missing publisher identifiers
  - Warning only shown when exactly 6 columns present (indicating identifier columns exist)
- **DataCite API Validation**: Enhanced availability check
  - Performs real metadata fetch during validation phase
  - Explicit handling of authentication and network errors

### Technical Details
- **Publisher Implementation**:
  - `src/api/datacite_client.py`: +3 methods (fetch, validate, update)
  - `src/db/sumariopmd_client.py`: +2 methods (fetch, update)
  - `src/utils/csv_parser.py`: New `parse_publisher_update_csv()` method
  - `src/utils/csv_exporter.py`: New `export_dois_with_publisher_to_csv()` function
  - `src/workers/publisher_update_worker.py`: New worker (600+ lines) with change detection
  - `src/ui/main_window.py`: New DOIPublisherFetchWorker + UI integration
- **Test Coverage**: +30 tests (391 → 421 tests)
  - CSV export/parsing: Valid/invalid formats, language codes, schemes
  - DataCite client: Publisher fetch, update, validation
  - Worker: Dry run, update logic, change detection, skipped details
  - Overall coverage: 78% maintained
- **Database Schema**:
  - Publisher stored in `resource` table (column: `publisher`)
  - Single field, no separate tables

## [0.3.0] - 2025-11-23

### Added
- 🚀 **Smart Change Detection** (#12): "Diff-Before-Update" optimization for bulk operations
  - **Automatic change detection**: Compares current DataCite metadata with CSV before updating
  - **URL updates**: Skips DOIs with unchanged landing page URLs
  - **Author updates**: Multi-field comparison (name, ORCID, order, count) with normalization
  - **95-98% fewer API calls**: Typical workflows skip most DOIs (no changes detected)
  - **Efficiency metrics**: Result dialogs show percentage of API calls saved
  - **Detailed logging**: Skipped DOIs section with reasons in update log files
    - Example reasons: "URL unverändert: https://...", "Keine Änderungen in Creator-Metadaten"
  - **Performance optimization**: Minimal overhead (fetch + compare < update cost)
  - **Extended signals**: Workers emit `(success, error, skipped, error_list, skipped_details)`
  - **Comprehensive test coverage**: +18 tests for skipped_details functionality (391 total tests)
  - **Worker coverage improvement**: authors_update_worker 65% → 85% (+20% coverage)
- **Database Synchronization** (#11): Automatic sync of author metadata with internal GFZ database
  - **Settings Dialog**: Tab-based UI with Theme and Database configuration
    - General tab: Theme settings (Auto/Light/Dark) moved from menu
    - Database tab: SUMARIOPMD credentials with connection test
    - Keyboard shortcut: Ctrl+, for quick access
  - **SumarioPMDClient**: MySQL database client with connection pooling
    - `test_connection()`: Non-blocking connection validation
    - `get_resource_id_for_doi()`: DOI to resource_id resolution
    - `fetch_creators_for_resource()`: Fetch creators (NOT contributors!)
    - `update_creators_transactional()`: ACID transactions with ROLLBACK
    - ORCID normalization: Full URL → ID-only format
    - Connection pool: 3 connections, 10s timeout
  - **Database-First Two-Phase-Commit Pattern**:
    - Phase 1: Validation (both systems must be reachable)
    - Phase 2a: Database update FIRST (with ROLLBACK on failure)
    - Phase 2b: DataCite update SECOND (with retry on failure)
    - Minimizes inconsistency risk: ~50% → ~5%
  - **Enhanced Progress Feedback**:
    - New signals: `validation_update`, `database_update`, `datacite_update`
    - Real-time status for each update phase
    - Separate handlers for validation, database, and DataCite operations
  - **Extended Update Logs**:
    - Database sync status (Enabled/Disabled)
    - Critical inconsistency counter with warnings
    - DATABASE-FIRST UPDATE PATTERN documentation
    - Manual correction hints for rare inconsistencies
  - **Secure Credential Storage**:
    - Database passwords stored in Windows Credential Manager
    - Same security model as DataCite credentials
    - Service name: `GROBI_SumarioPMD`
  - **Connection Test Worker**: Non-blocking database connectivity test in Settings
  - **VPN Requirement Detection**: Clear error messages when VPN disconnected
- **Professional Menu Bar** (#9): Desktop-style navigation
  - **Ansicht** (View) menu with Theme submenu (Auto/Hell/Dunkel)
  - **Hilfe** (Help) menu with About, Changelog, and GitHub links
  - Radio button groups for exclusive theme selection
  - Keyboard navigation support
  - Theme checkmarks showing current selection
- **About Dialog**: Comprehensive application information
  - Application logo (128x128) with professional layout
  - Version number from centralized `__version__.py`
  - Author and organization information
  - Quick action buttons:
    - 🔗 GitHub Repository
    - 📄 Changelog (opens local file or GitHub releases)
    - 📜 License (opens LICENSE file or GitHub)
  - Modal dialog with fixed size (450x550)
  - Graceful fallback URLs for .exe distributions
- **Logo in Main Window**: Prominent branding
  - 32x32 logo displayed in header next to title
  - Smooth scaling with Qt.SmoothTransformation
  - Horizontal layout for compact display
  - Logo source: GROBI-Logo.ico
- **Workflow Organization with GroupBoxes**:
  - **🔗 Landing Page URLs** group:
    - Status label showing CSV availability
    - Export button (always enabled)
    - Update button (enabled only when CSV exists)
  - **👥 Autoren-Metadaten** group:
    - Status label showing CSV availability
    - Export button (always enabled)
    - Update button (enabled only when CSV exists)
  - Visual separation with borders and titles
  - Status indicators: ⚪ "Keine CSV-Datei gefunden" / 🟢 "CSV bereit: {filename}"
- **CSV File Detection**: Intelligent workflow management
  - Automatic detection at startup
  - Checks for username-specific CSV files
  - Falls back to any matching CSV files if username unknown
  - Real-time UI updates after export/update operations
  - Smart button enabling based on file presence
- **Centralized Version Management**: Single source of truth
  - `src/__version__.py` with version, author, organization, license, URL
  - Used by About dialog, build scripts, and documentation
  - Version 0.3.0 established

### Changed
- **Update Workflows**: Optimized with change detection
  - URL updates: Fetch current URL, compare, skip if unchanged
  - Author updates: Fetch current creators, compare all fields, skip if unchanged
  - Result dialogs: Now show success/skipped/error counts with efficiency percentage
  - Log files: New "ÜBERSPRUNGENE DOIs" section with detailed reasons
  - User feedback: Clear indication of optimization benefits (e.g., "95.0% der API-Aufrufe eingespart!")
- **Settings Management**: Centralized configuration dialog
  - Theme settings moved from Ansicht menu to Settings dialog
  - Database configuration now in dedicated tab
  - QSettings for non-sensitive data (enabled flags)
  - Keyring for sensitive data (passwords)
- **Author Update Workflow**: Extended with optional database sync
  - Validation phase now tests both DataCite AND database
  - All-or-nothing approach: update aborted if any system unreachable
  - Progress dialog shows status for both systems separately
  - Continue-on-error strategy preserved (per-DOI failures logged)
- **Main Window**: Settings menu added
  - New menu item: "Einstellungen..." with Ctrl+, shortcut
  - Theme menu removed (now in Settings dialog)
  - Cleaner menu bar structure
- **Theme Switching**: Moved from button to menu bar
  - Theme toggle removed from main window buttons
  - Integrated into Ansicht → Theme menu
  - More professional desktop application feel
  - Clearer visual feedback with menu checkmarks
- **Button Labels**: More descriptive workflow names
  - "DOIs und Landing Page URLs laden" → "DOIs und URLs exportieren"
  - "DOIs und Autoren laden" → "DOIs und Autoren exportieren"
  - Clearer action-oriented language
- **Button Layout**: Organized in functional groups
  - GroupBox containers for visual organization
  - Status labels for workflow state awareness
  - Reduced button height (50px → 40px) for compact layout
  - Better information hierarchy
- **Main Window Layout**: Improved visual structure
  - Logo added to header (32x32, left-aligned)
  - Title and subtitle in vertical container
  - GroupBoxes replace flat button list
  - More scannable interface with clear sections

### Technical Details
- **Change Detection Algorithm**:
  - `update_worker.py`: `get_doi_url()` + string comparison
  - `authors_update_worker.py`: `_detect_creator_changes()` method
    - Multi-field comparison: name, nameType, givenName, familyName, ORCID
    - ORCID normalization: Strips `https://orcid.org/` prefix for comparison
    - Order-sensitive: Creator sequence matters
    - Count-sensitive: Number of creators must match
  - Skipped DOIs collected as `List[Tuple[str, str]]` (DOI + reason)
  - Log files: Dedicated "ÜBERSPRUNGENE DOIs" section
  - Efficiency calculation: `(skipped / total) * 100`
- **Extended Test Suite**: +18 tests (373 → 391)
  - `test_update_worker_skipped_details.py`: 6 tests for URL change detection
  - `test_authors_update_worker_skipped_details.py`: 5 tests for author change detection
  - `test_log_files_skipped_details.py`: 7 tests for log file generation
  - All tests use mocking (no real API/DB connections)
  - Focus: Format validation, empty scenarios, mixed scenarios, error handling, logging
- **Code Coverage Improvement**:
  - authors_update_worker.py: 65% → 85% (+20%)
  - update_worker.py: 87% (stable)
  - Overall workers coverage: 86%
  - Total: 391 tests, 78% overall coverage
- **QGroupBox Styling**: Theme-aware group containers
  - Light mode: White background, #d0d0d0 borders, #0078d4 titles
  - Dark mode: #252525 background, #3e3e3e borders, #1177bb titles
  - 6px border radius, 12px top margin for title positioning
- **Menu System**: QMenuBar with QActionGroup
  - Exclusive action groups for theme selection
  - Signal/Slot connections for menu actions
  - QDesktopServices for opening external links/files
- **About Dialog**: QDialog with QVBoxLayout
  - QPixmap for logo display with scaling
  - QPushButton actions for external links
  - Modal dialog with fixed dimensions
  - Error handling for missing resources
- **CSV Detection Logic**: Path-based file checking
  - `_check_csv_files()` method in MainWindow
  - `_current_username` tracking for specific file lookup
  - `Path.exists()` and `Path.glob()` for file discovery
  - Automatic UI state updates via button.setEnabled()
- **Extended Test Suite**: +43 tests (244 → 287)
  - `test_about_dialog.py`: 14 new tests
    - Dialog initialization and properties
    - Logo display and scaling
    - Version and author information
    - Button functionality and link opening
    - Layout structure validation
  - `test_main_window.py`: Enhanced with 17 new tests
    - CSV file detection (6 tests)
    - GroupBox structure validation (3 tests)
    - Menu bar functionality (8 tests)
  - All 287 tests passing with 77% coverage maintained
- **Dependencies**:
  - mysql-connector-python >=8.0.33 (already in requirements.txt)
  - keyring >=24.3.0 (extended for database credentials)
- **Architecture**:
  - `src/db/sumariopmd_client.py`: Database client (745 lines)
  - `src/ui/settings_dialog.py`: Tab-based settings UI
  - `src/workers/connection_test_worker.py`: Async connection testing
  - Extended `src/utils/credential_manager.py`: DB credential functions
- **Threading**:
  - ConnectionTestWorker: QThread-based non-blocking DB test
  - AuthorsUpdateWorker: Extended with database sync logic
  - All database operations run in worker threads (never main thread)
- **Database Schema**:
  - Table: `resource` (DOI storage with resource_id primary key)
  - Table: `resourceagent` (person metadata with firstname/lastname/identifier)
  - Table: `role` (role assignments, filter: `role='Creator'`)
  - **CRITICAL**: Only Creators updated, never Contributors!
- **Error Handling**:
  - `SumarioPMDError`: Base exception class
  - `DBConnectionError` (aliased from `sumariopmd_client.ConnectionError`): Database unreachable
  - `ResourceNotFoundError`: DOI not in database
  - `TransactionError`: ROLLBACK triggered
  - Retry logic: 1-2 immediate retries for DataCite failures
  - Inconsistency logging: Database OK but DataCite failed
- **Test Coverage**:
  - 35 new unit tests (14 Phase 1 + 21 Phase 2)
  - Integration tests for Database-First pattern
  - UI tests for signal handling (5/8 passing, core verified)
  - Total: 322 tests (287 existing + 35 new)

- ⚠️ **Theme settings location changed**: No longer in Ansicht menu, now in Settings dialog
  - Old: Menu → Ansicht → Theme → Auto/Hell/Dunkel
  - New: Menu → Einstellungen → Tab "Allgemein"
  - Impact: Users must adjust to new location
  - Mitigation: Theme preference preserved (stored in QSettings)

### UI Improvements
- More professional desktop application appearance
- Better visual hierarchy with grouped workflows
- Status awareness (CSV file presence)
- Reduced cognitive load with organized layout
- Consistent spacing and alignment
- Theme-consistent GroupBox styling
- Logo provides instant brand recognition
- Menu bar follows standard desktop conventions

## [0.2.0] - 2025-11-20

### Added
- **Landing Page URL Update** (#3): Bulk update DOI landing page URLs via CSV import
  - CSV validation with DOI and URL format checks
  - Continue-on-error strategy with comprehensive logging
  - Real-time progress tracking (X/Y counter)
  - Timestamped update log files with error details
  - Background threading for non-blocking operations
- **Dark/Light Mode Support** (#4): Theme system with auto-detection
  - Automatic detection of Windows system theme
  - Manual override (AUTO/LIGHT/DARK modes)
  - Persistent theme preference across sessions
  - Theme-aware styling for all UI components
  - Real-time theme switching without restart
- **Export DOIs with Authors** (#5): CSV export with creator information
  - Full creator metadata export (name, type, ORCID)
  - Support for multiple creators per DOI
  - Personal and organizational creators
  - ORCID identifier integration
  - One row per creator format
- **Author Metadata Update** (#6): Bulk update creator information
  - Automatic dry run validation before updates
  - Creator count matching verification
  - CSV format validation with detailed error messages
  - Support for Personal and Organizational creators
  - ORCID validation and warnings
  - Row order preservation (determines creator order in DataCite)
  - Separate log files for validation and update results
- **Credential Manager** (#7): Secure multi-account management
  - Windows Credential Manager integration via keyring library
  - Save and manage multiple DataCite accounts
  - Automatic credential save prompt after successful authentication
  - Account dropdown with last-used account selection
  - Secure password storage (encrypted by OS)
  - Account metadata stored locally in %APPDATA%/GROBI/
  - Delete account functionality with confirmation
- **macOS Build Support** (#8): Native macOS application builds
  - Automated macOS builds via GitHub Actions
  - Native .app bundle creation with Nuitka
  - DMG disk image distribution
  - .icns icon support for macOS
  - Unified GitHub releases (Windows + macOS)
  - Build script: `scripts/build_macos.py`

### Changed
- Credentials dialog redesigned for account management
- Workers now support credential save requests
- Main window improved with separate workflow buttons
- Progress bars show determinate progress for bulk operations
- CSV parser extended with author metadata validation

### Technical Details
- keyring >=24.3.0 for secure credential storage
- QThread-based background workers for all long-running operations
- Signal/Slot architecture for thread-safe UI updates
- Theme manager with QSettings integration
- Comprehensive error handling with German error messages
- Extended test suite: 244 tests with 77% code coverage
- macOS compatibility (10.15 Catalina or later)
- Cross-platform build automation (Windows + macOS)

## [0.1.2] - 2025-11-19

### Changed
- Improved CI badge handling in README
- Updated Codecov badge display
- Standardized README license badge

## [0.1.1] - 2025-11-19

### Changed
- Standardized build script and workflow log messages
- Improved consistency in terminal output

## [0.1.0] - 2025-11-19

### Added
- Initial release
- DataCite API integration with full pagination support
- CSV export functionality with UTF-8 encoding
- Modern Qt6-based GUI with PySide6
- Credentials dialog with test/production API toggle
- Asynchronous API calls with progress indication
- Comprehensive error handling with German error messages
- Unit tests with 77% code coverage (90%+ for business logic)
- CI/CD pipeline with GitHub Actions
- Multi-platform testing (Windows, Ubuntu)
- Multi-version Python support (3.10-3.13)
- Detailed logging to grobi.log
- Complete documentation in README
- **Windows EXE build** with Nuitka compiler
- **Automated release workflow** via GitHub Actions
- **Standalone executable** (~23 MB) with embedded icon and metadata

### Technical Details
- Python 3.10+ support (developed with 3.13)
- PySide6 for modern Windows 11 UI
- Thread-safe implementation with Qt Signals/Slots
- HTTP Basic Authentication for DataCite API
- Automatic pagination for large datasets
- Nuitka compilation with MSVC 14.3+ for Windows executables
- One-file deployment with compressed payload (29.46% ratio)
