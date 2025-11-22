# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2025-11-21

### Added
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
