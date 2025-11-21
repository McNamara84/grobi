"""Build script for creating GROBI.app for macOS with Nuitka.

This script automates the process of building a standalone macOS application
from the GROBI Python application using Nuitka compiler.

Requirements:
    - Nuitka installed (pip install -r requirements-build.txt)
    - All application dependencies installed
    - Icon file at src/ui/GROBI-Logo.icns (optional)
    - macOS (for .app bundle creation)

Usage:
    python scripts/build_macos.py
"""

import subprocess
import sys
import shutil
from pathlib import Path
from datetime import datetime

# Import version from package
sys.path.insert(0, str(Path(__file__).parent.parent))
from src import __version__, __copyright__


def print_header(text: str) -> None:
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")


def check_prerequisites() -> bool:
    """Check if all prerequisites are met."""
    print_header("Checking Prerequisites")
    
    errors = []
    warnings = []
    
    # Check platform
    if sys.platform != "darwin":
        errors.append("This script must be run on macOS")
        print("\n[ERROR] Prerequisites check failed:\n")
        for error in errors:
            print(f"  - {error}")
        return False
    
    print("[OK] Platform: macOS")
    
    # Check Nuitka installation
    try:
        result = subprocess.run(
            [sys.executable, "-m", "nuitka", "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        nuitka_version = result.stdout.strip().split()[0]
        print(f"[OK] Nuitka {nuitka_version} installed")
    except (subprocess.CalledProcessError, FileNotFoundError):
        errors.append("Nuitka not found. Install with: pip install -r requirements-build.txt")
    
    # Check icon file (optional for macOS)
    icon_path = Path("src/ui/GROBI-Logo.icns")
    if icon_path.exists():
        print(f"[OK] Icon file found: {icon_path}")
    else:
        warnings.append(f"Icon file not found: {icon_path} (app will use default icon)")
    
    # Check main.py
    main_path = Path("src/main.py")
    if main_path.exists():
        print(f"[OK] Entry point found: {main_path}")
    else:
        errors.append(f"Entry point not found: {main_path}")
    
    # Check PySide6 installation
    try:
        import PySide6
        print(f"[OK] PySide6 {PySide6.__version__} installed")
    except ImportError:
        errors.append("PySide6 not found. Install with: pip install -r requirements.txt")
    
    if warnings:
        print("\n[WARNING] Non-critical issues:\n")
        for warning in warnings:
            print(f"  - {warning}")
    
    if errors:
        print("\n[ERROR] Prerequisites check failed:\n")
        for error in errors:
            print(f"  - {error}")
        return False
    
    print("\n[OK] All prerequisites met!")
    return True


def clean_build_dirs() -> None:
    """Clean up previous build directories."""
    print_header("Cleaning Build Directories")
    
    dirs_to_clean = [
        Path("build"),
        Path("src/main.build"),
        Path("src/main.dist"),
        Path("src/main.onefile-build"),
    ]
    
    for dir_path in dirs_to_clean:
        if dir_path.exists():
            print(f"Removing {dir_path}...")
            shutil.rmtree(dir_path)
    
    print("[OK] Build directories cleaned")


def build_app() -> bool:
    """Build the application bundle with Nuitka."""
    print_header(f"Building GROBI.app v{__version__}")
    
    # Prepare output directory
    dist_dir = Path("dist")
    dist_dir.mkdir(exist_ok=True)
    
    # Base Nuitka command
    nuitka_args = [
        sys.executable, "-m", "nuitka",
        
        # macOS app bundle configuration
        "--standalone",
        "--macos-create-app-bundle",
        "--output-dir=dist",
        
        # Disable console (GUI app)
        "--disable-console",
        
        # Product information
        "--macos-app-name=GROBI",
        f"--macos-app-version={__version__}",
        
        # Plugin configuration
        "--enable-plugin=pyside6",
        
        # Optimization
        "--assume-yes-for-downloads",
        "--remove-output",  # Remove build directories after successful build
        
        # Progress display
        "--show-progress",
        "--show-memory",
        
        # Entry point
        "src/main.py"
    ]
    
    # Add icon if available
    icon_path = Path("src/ui/GROBI-Logo.icns")
    if icon_path.exists():
        nuitka_args.insert(-1, f"--macos-app-icon={icon_path}")
        print(f"Using icon: {icon_path}")
    
    print("Nuitka command:")
    print(" ".join(nuitka_args))
    print("\nStarting compilation (this may take 10-15 minutes)...\n")
    
    # Run Nuitka
    start_time = datetime.now()
    
    try:
        subprocess.run(
            nuitka_args,
            check=True
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print_header("Build Successful!")
        print(f"Build time: {duration}")
        
        # Check if app bundle was created
        app_path = dist_dir / "main.app"
        grobi_app_path = dist_dir / "GROBI.app"
        
        # Nuitka creates main.app, rename to GROBI.app
        if app_path.exists():
            if grobi_app_path.exists():
                shutil.rmtree(grobi_app_path)
            app_path.rename(grobi_app_path)
            print(f"App bundle renamed: {app_path} -> {grobi_app_path}")
        
        if grobi_app_path.exists():
            # Calculate size
            total_size = sum(f.stat().st_size for f in grobi_app_path.rglob('*') if f.is_file())
            size_mb = total_size / (1024 * 1024)
            print(f"App bundle created: {grobi_app_path}")
            print(f"Total size: {size_mb:.1f} MB")
            return True
        else:
            print(f"[ERROR] App bundle not found at {grobi_app_path}")
            print("Check Nuitka output above for errors.")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Build failed with exit code {e.returncode}")
        return False
    except KeyboardInterrupt:
        print("\n\n[ERROR] Build cancelled by user")
        return False


def create_build_info() -> None:
    """Create a build info file."""
    print_header("Creating Build Info")
    
    info_path = Path("dist/BUILD_INFO.txt")
    
    build_info = f"""GROBI Build Information
=======================

Version: {__version__}
Build Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Python Version: {sys.version.split()[0]}
Platform: macOS

Application Details:
- Name: GROBI
- Full Name: GFZ Repository Operations & Batch Interface
- Company: GFZ Helmholtz Centre for Geosciences
- Copyright: {__copyright__}
- License: GPLv3

Features:
- DataCite API Integration
- CSV Export
- Modern Qt6 GUI
- Thread-safe implementation

Usage:
1. Double-click GROBI.app to start
2. On first run, macOS may show "unidentified developer" warning
3. Go to System Settings → Privacy & Security → Allow GROBI
4. Or: Right-click GROBI.app → Open → Open anyway

Notes:
- The app is not code-signed (requires Apple Developer Program membership)
- Gatekeeper will show a warning on first launch
- The application creates grobi.log in ~/Library/Logs/GROBI/
- CSV files are saved in your Documents folder by default

macOS Compatibility:
- Requires macOS 10.15 (Catalina) or later
- Built with Python {sys.version.split()[0]}
- Intel and Apple Silicon supported

Support:
- GitHub: https://github.com/McNamara84/grobi
- Issues: https://github.com/McNamara84/grobi/issues
"""
    
    info_path.write_text(build_info, encoding="utf-8")
    print(f"[OK] Build info created: {info_path}")


def create_dmg() -> bool:
    """Create a DMG disk image for distribution (optional)."""
    print_header("Creating DMG Distribution")
    
    try:
        # Check if hdiutil is available (should be on all macOS systems)
        subprocess.run(["which", "hdiutil"], check=True, capture_output=True)
        
        dmg_path = Path(f"dist/GROBI-{__version__}-macOS.dmg")
        app_path = Path("dist/GROBI.app")
        
        # Remove old DMG if exists
        if dmg_path.exists():
            dmg_path.unlink()
        
        print("Creating DMG image...")
        
        # Create DMG with hdiutil
        subprocess.run([
            "hdiutil", "create",
            "-volname", f"GROBI {__version__}",
            "-srcfolder", str(app_path),
            "-ov",
            "-format", "UDZO",  # Compressed
            str(dmg_path)
        ], check=True)
        
        if dmg_path.exists():
            size_mb = dmg_path.stat().st_size / (1024 * 1024)
            print(f"[OK] DMG created: {dmg_path}")
            print(f"DMG size: {size_mb:.1f} MB")
            return True
        else:
            print("[WARNING] DMG creation may have failed")
            return False
            
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"[WARNING] Could not create DMG: {e}")
        print("App bundle is still available at dist/GROBI.app")
        return False


def main() -> int:
    """Main build process."""
    print_header("GROBI macOS Build Script")
    print(f"Version: {__version__}")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Platform: {sys.platform}")
    
    # Check prerequisites
    if not check_prerequisites():
        return 1
    
    # Clean previous builds
    clean_build_dirs()
    
    # Build application bundle
    if not build_app():
        return 1
    
    # Create build info
    create_build_info()
    
    # Create DMG (optional, won't fail build if unsuccessful)
    create_dmg()
    
    # Final summary
    print_header("Build Complete!")
    print("Your application is ready:")
    print("  -> dist/GROBI.app")
    
    dmg_path = Path(f"dist/GROBI-{__version__}-macOS.dmg")
    if dmg_path.exists():
        print("  -> " + str(dmg_path))
    
    print("\nTest it with:")
    print("  open dist/GROBI.app")
    print("\nNext steps:")
    print("  1. Test the application thoroughly")
    print("  2. Check that all features work")
    print(f"  3. Create a release with: git tag v{__version__}")
    print("  4. Push tag to trigger automated release")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
