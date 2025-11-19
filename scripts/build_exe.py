"""Build script for creating GROBI.exe with Nuitka.

This script automates the process of building a standalone Windows executable
from the GROBI Python application using Nuitka compiler.

Requirements:
    - Nuitka installed (pip install -r requirements-build.txt)
    - All application dependencies installed
    - Icon file at src/ui/GROBI-Logo.ico

Usage:
    python scripts/build_exe.py
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
    
    # Check icon file
    icon_path = Path("src/ui/GROBI-Logo.ico")
    if icon_path.exists():
        print(f"[OK] Icon file found: {icon_path}")
    else:
        errors.append(f"Icon file not found: {icon_path}")
    
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
        Path("dist"),
        Path("src/main.build"),
        Path("src/main.dist"),
        Path("src/main.onefile-build"),
    ]
    
    for dir_path in dirs_to_clean:
        if dir_path.exists():
            print(f"Removing {dir_path}...")
            shutil.rmtree(dir_path)
    
    print("[OK] Build directories cleaned")


def build_exe() -> bool:
    """Build the executable with Nuitka."""
    print_header(f"Building GROBI v{__version__}")
    
    # Prepare output directory
    dist_dir = Path("dist")
    dist_dir.mkdir(exist_ok=True)
    
    # Nuitka command with all parameters
    nuitka_args = [
        sys.executable, "-m", "nuitka",
        
        # Output configuration
        "--onefile",
        "--output-dir=dist",
        "--output-filename=GROBI.exe",
        
        # Windows configuration
        "--windows-disable-console",
        "--windows-icon-from-ico=src/ui/GROBI-Logo.ico",
        
        # Company and product information
        "--company-name=GFZ Helmholtz Centre for Geosciences",
        "--product-name=GROBI",
        f"--file-version={__version__}",
        f"--product-version={__version__}",
        "--file-description=GFZ Repository Operations & Batch Interface",
        f"--copyright={__copyright__}",
        
        # Plugin configuration
        "--enable-plugin=pyside6",
        
        # Compiler configuration
        "--msvc=latest",  # Use latest MSVC version for best compatibility
        
        # Optimization
        "--assume-yes-for-downloads",
        "--remove-output",  # Remove build directories after successful build
        
        # Progress display
        "--show-progress",
        "--show-memory",
        
        # Entry point
        "src/main.py"
    ]
    
    print("Nuitka command:")
    print(" ".join(nuitka_args))
    print("\nStarting compilation (this may take 10-15 minutes)...\n")
    
    # Run Nuitka
    start_time = datetime.now()
    
    try:
        # Run Nuitka - output goes to console (no sensitive info in standard build)
        subprocess.run(
            nuitka_args,
            check=True
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print_header("Build Successful!")
        print(f"Build time: {duration}")
        
        # Check if exe was created
        exe_path = dist_dir / "GROBI.exe"
        if exe_path.exists():
            size_mb = exe_path.stat().st_size / (1024 * 1024)
            print(f"EXE created: {exe_path}")
            print(f"File size: {size_mb:.1f} MB")
            return True
        else:
            print(f"[ERROR] EXE file not found at {exe_path}")
            print("Check Nuitka output above for errors.")
            print(f"Expected location: {exe_path.absolute()}")
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
Platform: Windows

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
1. Double-click GROBI.exe to start
2. The application creates grobi.log in the same directory
3. CSV files are saved in the same directory

Notes:
- First run may trigger Windows SmartScreen (unsigned executable)
- Click "More info" -> "Run anyway" to proceed
- Some antivirus software may flag the executable (false positive)

Support:
- GitHub: https://github.com/McNamara84/grobi
- Issues: https://github.com/McNamara84/grobi/issues
"""
    
    info_path.write_text(build_info, encoding="utf-8")
    print(f"[OK] Build info created: {info_path}")


def main() -> int:
    """Main build process."""
    print_header("GROBI EXE Build Script")
    print(f"Version: {__version__}")
    print(f"Python: {sys.version.split()[0]}")
    
    # Check prerequisites
    if not check_prerequisites():
        return 1
    
    # Clean previous builds
    clean_build_dirs()
    
    # Build executable
    if not build_exe():
        return 1
    
    # Create build info
    create_build_info()
    
    # Final summary
    print_header("Build Complete!")
    print("Your executable is ready:")
    print("  -> dist/GROBI.exe")
    print("\nTest it with:")
    print("  .\\dist\\GROBI.exe")
    print("\nNext steps:")
    print("  1. Test the executable thoroughly")
    print("  2. Check that all features work")
    print(f"  3. Create a release with: git tag v{__version__}")
    print("  4. Push tag to trigger automated release")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
