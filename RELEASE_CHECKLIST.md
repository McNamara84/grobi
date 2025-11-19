# Release Checklist

Use this checklist for creating new releases of GROBI.

## Pre-Release Preparation

### 1. Version Update
- [ ] Update version in `src/__init__.py`
  ```python
  __version__ = "X.Y.Z"
  ```
- [ ] Verify version follows [Semantic Versioning](https://semver.org/)
  - MAJOR: Breaking changes
  - MINOR: New features (backward compatible)
  - PATCH: Bug fixes (backward compatible)

### 2. Testing
- [ ] Run full test suite
  ```bash
  pytest
  ```
- [ ] Verify test coverage (should be ≥75%)
  ```bash
  pytest --cov=src --cov-report=term
  ```
- [ ] Manual GUI testing:
  - [ ] Application starts without errors
  - [ ] Credentials dialog opens correctly
  - [ ] Test API connection works
  - [ ] Production API connection works
  - [ ] DOI retrieval functions properly
  - [ ] CSV export creates valid files
  - [ ] Error handling displays appropriate messages
  - [ ] Logging to grobi.log works

### 3. Local Build Test
- [ ] Build EXE locally
  ```bash
  python scripts/build_exe.py
  ```
- [ ] Verify EXE size (~23 MB)
- [ ] Test EXE functionality:
  - [ ] Starts without console window
  - [ ] Icon displays correctly
  - [ ] All features work as expected
  - [ ] No error messages in output

### 4. Documentation
- [ ] Update `CHANGELOG.md`
  - [ ] Move items from `[Unreleased]` to new version section
  - [ ] Add release date: `## [X.Y.Z] - YYYY-MM-DD`
  - [ ] List all Added, Changed, Fixed, Removed items
  - [ ] Include technical details if relevant

- [ ] Verify `README.md` is current
  - [ ] Installation instructions accurate
  - [ ] Usage examples work
  - [ ] Links are valid
  - [ ] Screenshots up-to-date (if any)

- [ ] Check `BUILD_INFO.txt` will be generated correctly

### 5. Repository Status
- [ ] All changes committed
  ```bash
  git status
  ```
- [ ] On correct branch (usually `main` or `develop`)
- [ ] Branch is up-to-date with remote
  ```bash
  git pull origin main
  ```
- [ ] No merge conflicts

## Release Process

### 6. Create Release Commit
```bash
# Commit version updates
git add src/__init__.py CHANGELOG.md
git commit -m "Release vX.Y.Z"
```

### 7. Create and Push Tag
```bash
# Create annotated tag
git tag -a vX.Y.Z -m "Release version X.Y.Z"

# Push commit and tag
git push origin main
git push origin vX.Y.Z
```

### 8. Monitor GitHub Actions
- [ ] Go to [Actions tab](https://github.com/McNamara84/grobi/actions)
- [ ] Wait for "Build Windows EXE" workflow to complete (~4-5 minutes)
- [ ] Verify workflow succeeded (green checkmark)
- [ ] Check workflow logs for any warnings

### 9. Verify GitHub Release
- [ ] Go to [Releases page](https://github.com/McNamara84/grobi/releases)
- [ ] Verify new release was created automatically
- [ ] Check release assets are present:
  - [ ] `GROBI-vX.Y.Z-Windows.zip`
  - [ ] `GROBI.exe`
  - [ ] `BUILD_INFO.txt`
- [ ] Download and test the release assets:
  - [ ] ZIP extracts correctly
  - [ ] EXE runs without errors
  - [ ] All features work as expected

## Post-Release

### 10. Update Development Version
```bash
# Update version to next development version
# e.g., if released 0.1.0, update to 0.2.0-dev
# Edit src/__init__.py

git add src/__init__.py
git commit -m "Bump version to X.Y.Z-dev"
git push origin main
```

### 11. Announcement (Optional)
- [ ] Update project description if needed
- [ ] Announce release in relevant channels
- [ ] Update any external documentation

### 12. Cleanup
- [ ] Delete local build artifacts if needed
  ```bash
  Remove-Item -Path dist -Recurse -Force
  ```
- [ ] Archive old releases (if repository size becomes an issue)

## Troubleshooting

### Build Fails on GitHub Actions
1. Check workflow logs for error messages
2. Common issues:
   - **MSVC error**: Python version compatibility (use 3.12 in workflow)
   - **Import error**: Missing dependency in requirements.txt
   - **File not found**: Check file paths in build script
3. If build fails, tag can be deleted and recreated:
   ```bash
   git tag -d vX.Y.Z
   git push origin :refs/tags/vX.Y.Z
   ```

### Release Not Created
1. Verify tag format: `vX.Y.Z` (must start with 'v')
2. Check repository permissions (need `contents: write`)
3. Verify `GITHUB_TOKEN` has required permissions

### EXE Doesn't Work
1. Test on clean Windows system (without Python installed)
2. Check Windows Defender/Antivirus logs
3. Verify all dependencies are included in Nuitka build
4. Check icon file exists: `src/ui/GROBI-Logo.ico`

## Version History

Track major releases:

| Version | Date | Notes |
|---------|------|-------|
| 0.1.0 | 2025-11-19 | Initial release with EXE build |

## Notes

- **Always test locally before pushing a tag**
- **Tags are permanent** - avoid deleting tags after public release
- **Semantic Versioning** - maintain version consistency
- **GitHub Actions** - builds are cached, first build may take longer
- **Release Assets** - are stored indefinitely by GitHub

## Quick Commands

```bash
# Full release process (after all checks pass)
git add .
git commit -m "Release vX.Y.Z"
git tag -a vX.Y.Z -m "Release version X.Y.Z"
git push origin main
git push origin vX.Y.Z

# Check release status
git tag -l
git log --oneline --decorate

# View remote tags
git ls-remote --tags origin
```
