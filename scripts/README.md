# Build Scripts

This directory contains scripts for building and packaging GROBI.

## build_exe.py

Builds a standalone Windows executable using Nuitka.

### Prerequisites

1. Install build dependencies:
   ```bash
   pip install -r requirements-build.txt
   ```

2. Ensure all application dependencies are installed:
   ```bash
   pip install -r requirements.txt
   ```

3. Verify icon file exists at `src/ui/GROBI-Logo.ico`

### Usage

Run the build script:
```bash
python scripts/build_exe.py
```

### What it does

1. **Prerequisites Check**
   - Verifies Nuitka is installed
   - Checks for icon file
   - Validates entry point exists
   - Confirms PySide6 is available

2. **Clean Build**
   - Removes previous build artifacts
   - Cleans dist/ and build/ directories

3. **Nuitka Compilation**
   - Compiles Python to C code
   - Creates standalone executable
   - Embeds icon and metadata
   - Takes 10-15 minutes on first run

4. **Build Info**
   - Creates BUILD_INFO.txt with details
   - Documents version and build date

### Output

- `dist/GROBI.exe` - Standalone executable (~100 MB)
- `dist/BUILD_INFO.txt` - Build information

### Build Time

- **First build**: 10-15 minutes (Nuitka downloads and caches dependencies)
- **Subsequent builds**: 5-8 minutes (using cache)

### Testing the EXE

```bash
.\dist\GROBI.exe
```

### Troubleshooting

**"Nuitka not found"**
```bash
pip install -r requirements-build.txt
```

**"Icon file not found"**
- Verify `src/ui/GROBI-Logo.ico` exists
- Check file is a valid .ico format

**"Build failed"**
- Check all dependencies are installed
- Try cleaning build directories manually
- Run with verbose output: Add `--verbose` to nuitka_args in script

**Windows Defender blocks execution**
- This is normal for unsigned executables
- Right-click → Properties → "Unblock" checkbox
- Or click "More info" → "Run anyway" when starting

### Notes

- The script reads version from `src/__init__.py`
- Metadata (company, copyright) is embedded in EXE properties
- Build artifacts are gitignored automatically
- No console window appears when running the EXE
