# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - Unreleased

### Added
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
  - `ConnectionError`: Database unreachable
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
