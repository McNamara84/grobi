# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
