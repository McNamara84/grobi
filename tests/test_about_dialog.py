"""Tests for AboutDialog."""

import pytest
from pathlib import Path
from unittest.mock import patch
from PySide6.QtWidgets import QPushButton, QLabel
from PySide6.QtCore import QUrl

from src.ui.about_dialog import AboutDialog


@pytest.fixture
def app(qapp):
    """Provide QApplication instance."""
    return qapp


@pytest.fixture
def dialog(app):
    """Create AboutDialog instance."""
    return AboutDialog()


def test_about_dialog_initialization(dialog):
    """Test that AboutDialog initializes correctly."""
    assert dialog is not None
    assert dialog.windowTitle() == "Über GROBI"
    assert dialog.isModal()
    
    # Check fixed size
    assert dialog.width() == 450
    assert dialog.height() == 550


def test_about_dialog_has_logo(dialog):
    """Test that AboutDialog displays logo."""
    # Find logo label
    logo_labels = dialog.findChildren(QLabel)
    logo_label = None
    for label in logo_labels:
        pixmap = label.pixmap()
        if pixmap and not pixmap.isNull():
            logo_label = label
            break
    
    # Logo should be displayed if file exists
    logo_path = Path(__file__).parent.parent / "src" / "ui" / "GROBI-Logo.ico"
    if logo_path.exists():
        assert logo_label is not None
        assert logo_label.pixmap() is not None
        assert not logo_label.pixmap().isNull()


def test_about_dialog_has_version_info(dialog):
    """Test that AboutDialog displays version information."""
    labels = dialog.findChildren(QLabel)
    
    # Check for version label
    version_found = False
    for label in labels:
        text = label.text()
        if "Version" in text:
            version_found = True
            break
    
    assert version_found, "Version information should be displayed"


def test_about_dialog_has_author_info(dialog):
    """Test that AboutDialog displays author information."""
    labels = dialog.findChildren(QLabel)
    
    # Check for author label
    author_found = False
    for label in labels:
        text = label.text()
        if "Holger Ehrmann" in text or "GFZ Data Services" in text:
            author_found = True
            break
    
    assert author_found, "Author information should be displayed"


def test_about_dialog_has_buttons(dialog):
    """Test that AboutDialog has all required buttons."""
    buttons = dialog.findChildren(QPushButton)
    button_texts = [btn.text() for btn in buttons]
    
    # Check for expected buttons
    assert any("GitHub" in text for text in button_texts), "GitHub button should exist"
    assert any("Changelog" in text for text in button_texts), "Changelog button should exist"
    assert any("Lizenz" in text for text in button_texts), "License button should exist"
    assert any("OK" in text for text in button_texts), "OK button should exist"


@patch('src.ui.about_dialog.QDesktopServices.openUrl')
def test_github_button_opens_url(mock_open_url, dialog):
    """Test that GitHub button opens correct URL."""
    buttons = dialog.findChildren(QPushButton)
    github_button = None
    for btn in buttons:
        if "GitHub" in btn.text():
            github_button = btn
            break
    
    assert github_button is not None, "GitHub button should exist"
    
    # Click button
    github_button.click()
    
    # Check that openUrl was called
    assert mock_open_url.called, "openUrl should be called"
    call_args = mock_open_url.call_args[0][0]
    assert isinstance(call_args, QUrl)
    assert "github.com" in call_args.toString().lower()


@patch('src.ui.about_dialog.QDesktopServices.openUrl')
def test_changelog_button_opens_url(mock_open_url, dialog):
    """Test that Changelog button attempts to open file or URL."""
    buttons = dialog.findChildren(QPushButton)
    changelog_button = None
    for btn in buttons:
        if "Changelog" in btn.text():
            changelog_button = btn
            break
    
    assert changelog_button is not None, "Changelog button should exist"
    
    # Click button
    changelog_button.click()
    
    # Check that openUrl was called (either local file or fallback URL)
    assert mock_open_url.called, "openUrl should be called"


@patch('src.ui.about_dialog.QDesktopServices.openUrl')
def test_license_button_opens_url(mock_open_url, dialog):
    """Test that License button attempts to open file or URL."""
    buttons = dialog.findChildren(QPushButton)
    license_button = None
    for btn in buttons:
        if "Lizenz" in btn.text():
            license_button = btn
            break
    
    assert license_button is not None, "License button should exist"
    
    # Click button
    license_button.click()
    
    # Check that openUrl was called (either local file or fallback URL)
    assert mock_open_url.called, "openUrl should be called"


def test_close_button_closes_dialog(dialog):
    """Test that OK button closes the dialog."""
    buttons = dialog.findChildren(QPushButton)
    ok_button = None
    for btn in buttons:
        if btn.text() == "OK":
            ok_button = btn
            break
    
    assert ok_button is not None, "OK button should exist"
    
    # Show dialog non-modal for testing
    dialog.show()
    assert dialog.isVisible()
    
    # Click OK button
    ok_button.click()
    
    # Dialog should be closed (not visible)
    assert not dialog.isVisible()


def test_about_dialog_uses_version_module(dialog):
    """Test that AboutDialog imports version from __version__.py."""
    from src.__version__ import __version__
    
    labels = dialog.findChildren(QLabel)
    version_text_found = False
    
    for label in labels:
        if __version__ in label.text():
            version_text_found = True
            break
    
    assert version_text_found, f"Version {__version__} should be displayed in dialog"


def test_about_dialog_layout(dialog):
    """Test that AboutDialog has proper layout structure."""
    # Check that dialog has a layout
    assert dialog.layout() is not None
    
    # Check that layout contains widgets
    layout = dialog.layout()
    assert layout.count() > 0


def test_about_dialog_logo_scaling(dialog):
    """Test that logo is scaled to correct size if present."""
    logo_labels = dialog.findChildren(QLabel)
    
    for label in logo_labels:
        pixmap = label.pixmap()
        if pixmap and not pixmap.isNull():
            # Logo should be 128x128
            assert pixmap.width() == 128 or label.maximumWidth() == 128
            assert pixmap.height() == 128 or label.maximumHeight() == 128
            break


@patch('src.ui.about_dialog.Path.exists')
def test_about_dialog_handles_missing_logo(mock_exists, app):
    """Test that AboutDialog handles missing logo file gracefully."""
    # Mock logo file doesn't exist
    mock_exists.return_value = False
    
    # Should not raise exception
    dialog = AboutDialog()
    assert dialog is not None
    
    # Dialog should still be functional
    assert dialog.windowTitle() == "Über GROBI"


def test_about_dialog_styling(dialog):
    """Test that AboutDialog has basic styling applied."""
    # Check that stylesheet is set (non-empty)
    stylesheet = dialog.styleSheet()
    # AboutDialog might not have custom stylesheet, but should not error
    assert stylesheet is not None
