# Build script with temporary Windows Defender exclusion
# Must be run as Administrator

param(
    [switch]$SkipDefender
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path $PSScriptRoot -Parent
$distPath = Join-Path $projectRoot "dist"

Write-Host "=== GROBI Build with Defender Exclusion ===" -ForegroundColor Cyan
Write-Host "Project: $projectRoot" -ForegroundColor Gray
Write-Host "Dist: $distPath" -ForegroundColor Gray
Write-Host ""

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin -and -not $SkipDefender) {
    Write-Host "ERROR: This script requires Administrator privileges to configure Windows Defender." -ForegroundColor Red
    Write-Host ""
    Write-Host "Please run one of the following:" -ForegroundColor Yellow
    Write-Host "  1. Right-click PowerShell and 'Run as Administrator', then run this script" -ForegroundColor Yellow
    Write-Host "  2. Run with -SkipDefender flag (build may fail due to Defender)" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}

# Add Defender exclusion
if (-not $SkipDefender) {
    Write-Host "[1/4] Adding Windows Defender exclusion for dist folder..." -ForegroundColor Cyan
    try {
        Add-MpPreference -ExclusionPath $distPath -ErrorAction Stop
        Write-Host "✓ Defender exclusion added" -ForegroundColor Green
    } catch {
        Write-Host "⚠ Warning: Could not add Defender exclusion: $_" -ForegroundColor Yellow
        Write-Host "  Build may fail. Consider disabling Windows Defender temporarily." -ForegroundColor Yellow
    }
    Write-Host ""
}

# Clean old build artifacts
Write-Host "[2/4] Cleaning old build artifacts..." -ForegroundColor Cyan
if (Test-Path "$distPath\main.exe") {
    Remove-Item "$distPath\main.exe" -Force -ErrorAction SilentlyContinue
    Write-Host "  Removed main.exe" -ForegroundColor Gray
}
if (Test-Path "$distPath\GROBI.exe") {
    Remove-Item "$distPath\GROBI.exe" -Force -ErrorAction SilentlyContinue
    Write-Host "  Removed GROBI.exe" -ForegroundColor Gray
}
if (Test-Path "$distPath\main.build") {
    Remove-Item "$distPath\main.build" -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "  Removed main.build" -ForegroundColor Gray
}
if (Test-Path "$distPath\main.dist") {
    Remove-Item "$distPath\main.dist" -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "  Removed main.dist" -ForegroundColor Gray
}
if (Test-Path "$distPath\main.onefile-build") {
    Remove-Item "$distPath\main.onefile-build" -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "  Removed main.onefile-build" -ForegroundColor Gray
}
Write-Host "✓ Cleanup complete" -ForegroundColor Green
Write-Host ""

# Run build
Write-Host "[3/4] Running Nuitka build..." -ForegroundColor Cyan
Write-Host "  This will take 5-10 minutes..." -ForegroundColor Gray
Write-Host ""

Push-Location $projectRoot
try {
    & ".\.venv\Scripts\python.exe" "scripts\build_exe.py"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "✓ Build completed successfully" -ForegroundColor Green
        
        # Rename main.exe to GROBI.exe
        if (Test-Path "$distPath\main.exe") {
            Move-Item "$distPath\main.exe" "$distPath\GROBI.exe" -Force
            Write-Host "✓ Created GROBI.exe" -ForegroundColor Green
        }
    } else {
        Write-Host ""
        Write-Host "✗ Build failed with exit code $LASTEXITCODE" -ForegroundColor Red
    }
} finally {
    Pop-Location
}

Write-Host ""

# Remove Defender exclusion
if (-not $SkipDefender -and $isAdmin) {
    Write-Host "[4/4] Removing Windows Defender exclusion..." -ForegroundColor Cyan
    try {
        Remove-MpPreference -ExclusionPath $distPath -ErrorAction Stop
        Write-Host "✓ Defender exclusion removed" -ForegroundColor Green
    } catch {
        Write-Host "⚠ Warning: Could not remove Defender exclusion: $_" -ForegroundColor Yellow
        Write-Host "  You may want to remove it manually from Windows Security settings." -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "=== Build Complete ===" -ForegroundColor Cyan

if (Test-Path "$distPath\GROBI.exe") {
    $exeSize = (Get-Item "$distPath\GROBI.exe").Length / 1MB
    Write-Host "✓ GROBI.exe created: $([math]::Round($exeSize, 1)) MB" -ForegroundColor Green
    Write-Host "  Location: $distPath\GROBI.exe" -ForegroundColor Gray
} else {
    Write-Host "✗ GROBI.exe not found - build may have failed" -ForegroundColor Red
}
