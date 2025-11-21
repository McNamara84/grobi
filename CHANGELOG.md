# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned
- Modernized UI with logo in main window
- About dialog with version information
- Menu bar for professional desktop experience
- Workflow grouping with status indicators
- Improved button layout and organization

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
