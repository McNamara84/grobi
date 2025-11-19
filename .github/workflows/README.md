# GitHub Actions Workflows

This directory contains automated workflows for GROBI.

## Available Workflows

### 1. Build Windows EXE (`build-exe.yml`)

Automatically builds a standalone Windows executable using Nuitka when a new version tag is pushed.

**Triggers:**
- Push of version tags (e.g., `v0.1.0`, `v1.0.0`, `v2.3.4`)
- Manual trigger via GitHub Actions UI (workflow_dispatch)

**What it does:**
1. Sets up Python 3.12 environment (for MSVC compatibility)
2. Installs all dependencies
3. Builds GROBI.exe using Nuitka with MSVC
4. Creates a distribution package with documentation
5. Uploads artifacts to GitHub Actions
6. Creates a GitHub Release (only for version tags) with:
   - `GROBI-vX.X.X-Windows.zip` - Complete package
   - `GROBI.exe` - Standalone executable
   - `BUILD_INFO.txt` - Build information
   - `QUICK_START.txt` - Quick start guide

**Requirements:**
- GitHub repository with proper permissions
- Version tags following semantic versioning (vX.X.X)

**Usage:**

To trigger a new build and release:

```bash
# 1. Update version in src/__init__.py
# 2. Update CHANGELOG.md
# 3. Commit changes
git add .
git commit -m "Release v0.2.0"

# 4. Create and push tag
git tag -a v0.2.0 -m "Release version 0.2.0"
git push origin main
git push origin v0.2.0

# The workflow will automatically:
# - Build the EXE
# - Create a GitHub Release
# - Upload all assets
```

**Manual Trigger:**

You can also trigger the workflow manually from the GitHub Actions tab:
1. Go to "Actions" tab in GitHub
2. Select "Build Windows EXE" workflow
3. Click "Run workflow"
4. Select branch and click "Run workflow"

Note: Manual triggers will create artifacts but NOT create a GitHub Release (releases are only created for version tags).

**Build Environment:**
- OS: Windows Server (latest)
- Python: 3.12.x (for MSVC compatibility, development uses 3.13)
- Compiler: MSVC 14.3+ (Visual Studio 2022)
- Build time: ~4-5 minutes

**Output:**
- Artifact name: `GROBI-Windows-EXE`
- Retention: 90 days
- Release assets: Automatically attached to GitHub Release (when triggered by tag)

### 2. Tests (`tests.yml`)

Runs the test suite on every push and pull request.

**Triggers:**
- Push to any branch
- Pull requests

**What it does:**
1. Runs pytest with coverage
2. Uploads coverage report
3. Updates coverage badge

## Workflow Files Location

All workflow files must be in `.github/workflows/` directory and use `.yml` or `.yaml` extension.

## Secrets Required

No additional secrets are required beyond the default `GITHUB_TOKEN` which is automatically provided by GitHub Actions.

## Troubleshooting

### Build fails with MSVC error

The workflow uses Python 3.12 which has better MSVC compatibility. If you encounter MSVC-related errors:
- Check if the MSVC version is compatible
- Verify the `--msvc=latest` flag is used in Nuitka command
- Ensure Windows SDK is available on the runner

### Release not created

Ensure:
- The tag follows the pattern `v*.*.*` (e.g., `v0.1.0`)
- The tag starts with 'v' (checked by workflow condition)
- The tag is pushed to GitHub: `git push origin v0.1.0`
- Repository has proper permissions for creating releases
- Workflow was triggered by a tag push (not manual dispatch)

### Artifact upload fails

Check:
- File paths in the workflow match actual build output
- Files exist in the expected directories
- Workflow has proper permissions

### Tag validation warnings

If you see "Warning: Not a version tag", this means the workflow was triggered manually or by a branch push. The build will still complete, but no release will be created.

## Best Practices

1. **Version Tags**: Always use semantic versioning (MAJOR.MINOR.PATCH)
2. **Testing**: Ensure all tests pass before creating a release tag
3. **Changelog**: Update CHANGELOG.md before releasing
4. **Version Consistency**: Keep version in sync across:
   - `src/__init__.py`
   - Git tag
   - CHANGELOG.md

## Monitoring

You can monitor workflow runs:
1. Go to "Actions" tab in GitHub repository
2. Click on any workflow run to see details
3. Check logs for each step
4. Download artifacts from successful runs

## Caching

The workflow caches pip dependencies to speed up builds:
- Cache key includes Python version (3.12) and requirements files
- Cache is automatically invalidated when dependencies change
- Reduces build time by ~1-2 minutes

## Shell Specifications

All PowerShell steps explicitly use `shell: pwsh` for consistency and maintainability. This ensures:
- Correct shell is used on all runners
- Consistent behavior across different environments
- Better error messages when shell-specific commands fail
