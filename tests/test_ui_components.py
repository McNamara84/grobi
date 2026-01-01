"""Tests for the new UI components (ActionCard, SplitButton, CollapsibleSection)."""

import pytest
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout
from PySide6.QtCore import Qt

from src.ui.components import ActionCard, SplitButton, CollapsibleSection


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication for tests."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


class TestSplitButton:
    """Tests for the SplitButton component."""
    
    def test_initialization(self, qapp, qtbot):
        """Test basic initialization."""
        btn = SplitButton("Test Button")
        qtbot.addWidget(btn)
        
        assert btn.text() == "Test Button"
        assert btn.isEnabled()
    
    def test_initialization_with_icon(self, qapp, qtbot):
        """Test initialization with icon."""
        btn = SplitButton("Export", icon="📥")
        qtbot.addWidget(btn)
        
        assert btn.text() == "Export"
        assert "📥" in btn.primary_button.text()
    
    def test_add_action(self, qapp, qtbot):
        """Test adding actions to dropdown menu."""
        btn = SplitButton("Export")
        qtbot.addWidget(btn)
        
        # Dropdown should be hidden initially
        assert btn.dropdown_button.isHidden()
        
        action = btn.add_action("Update", "update", "🔄")
        
        # Dropdown should not be hidden after adding action
        assert not btn.dropdown_button.isHidden()
        assert action.data() == "update"
        assert "🔄" in action.text()
    
    def test_clear_actions(self, qapp, qtbot):
        """Test clearing all actions."""
        btn = SplitButton("Export")
        qtbot.addWidget(btn)
        
        btn.add_action("Action 1", "a1")
        btn.add_action("Action 2", "a2")
        assert not btn.dropdown_button.isHidden()
        
        btn.clear_actions()
        assert btn.dropdown_button.isHidden()
    
    def test_clicked_signal(self, qapp, qtbot):
        """Test that clicked signal is emitted."""
        btn = SplitButton("Export")
        qtbot.addWidget(btn)
        
        with qtbot.waitSignal(btn.clicked, timeout=1000):
            btn.primary_button.click()
    
    def test_action_triggered_signal(self, qapp, qtbot):
        """Test that action_triggered signal is emitted."""
        btn = SplitButton("Export")
        qtbot.addWidget(btn)
        
        action = btn.add_action("Update", "update")
        
        with qtbot.waitSignal(btn.action_triggered, timeout=1000):
            action.trigger()
    
    def test_set_enabled(self, qapp, qtbot):
        """Test enabling/disabling the button."""
        btn = SplitButton("Export")
        qtbot.addWidget(btn)
        btn.add_action("Update", "update")
        
        btn.setEnabled(False)
        assert not btn.primary_button.isEnabled()
        assert not btn.dropdown_button.isEnabled()
        
        btn.setEnabled(True)
        assert btn.primary_button.isEnabled()
        assert btn.dropdown_button.isEnabled()
    
    def test_set_action_enabled(self, qapp, qtbot):
        """Test enabling/disabling specific actions."""
        btn = SplitButton("Export")
        qtbot.addWidget(btn)
        
        btn.add_action("Action 1", "a1")
        btn.add_action("Action 2", "a2")
        
        btn.set_action_enabled("a1", False)
        
        for action in btn.menu.actions():
            if action.data() == "a1":
                assert not action.isEnabled()
            elif action.data() == "a2":
                assert action.isEnabled()


class TestCollapsibleSection:
    """Tests for the CollapsibleSection component."""
    
    def test_initialization_expanded(self, qapp, qtbot):
        """Test initialization in expanded state."""
        section = CollapsibleSection("Test Section", expanded=True)
        qtbot.addWidget(section)
        
        assert section.title() == "Test Section"
        assert section.is_expanded()
    
    def test_initialization_collapsed(self, qapp, qtbot):
        """Test initialization in collapsed state."""
        section = CollapsibleSection("Test Section", expanded=False)
        qtbot.addWidget(section)
        
        assert not section.is_expanded()
        assert section.content_area.maximumHeight() == 0
    
    def test_toggle(self, qapp, qtbot):
        """Test toggling the section."""
        section = CollapsibleSection("Test Section", expanded=True)
        qtbot.addWidget(section)
        
        assert section.is_expanded()
        
        section.toggle()
        # After toggle, should be collapsed
        assert not section.is_expanded()
        
        section.toggle()
        # After second toggle, should be expanded again
        assert section.is_expanded()
    
    def test_toggled_signal(self, qapp, qtbot):
        """Test that toggled signal is emitted."""
        section = CollapsibleSection("Test Section", expanded=True)
        qtbot.addWidget(section)
        
        with qtbot.waitSignal(section.toggled, timeout=1000) as blocker:
            section.toggle()
        
        assert blocker.args == [False]  # Collapsed
    
    def test_set_title(self, qapp, qtbot):
        """Test setting the title."""
        section = CollapsibleSection("Original Title")
        qtbot.addWidget(section)
        
        section.set_title("New Title")
        assert section.title() == "New Title"
        assert section.title_label.text() == "New Title"
    
    def test_add_widget(self, qapp, qtbot):
        """Test adding a widget to the content area."""
        section = CollapsibleSection("Test Section")
        qtbot.addWidget(section)
        
        test_widget = QWidget()
        section.add_widget(test_widget)
        
        # Widget should be added to the content layout
        assert test_widget.parent() is not None
    
    def test_set_expanded_without_animation(self, qapp, qtbot):
        """Test setting expanded state without animation."""
        section = CollapsibleSection("Test Section", expanded=True)
        qtbot.addWidget(section)
        
        section.set_expanded(False, animate=False)
        assert not section.is_expanded()
        assert section.content_area.maximumHeight() == 0
        
        section.set_expanded(True, animate=False)
        assert section.is_expanded()


class TestActionCard:
    """Tests for the ActionCard component."""
    
    def test_initialization(self, qapp, qtbot):
        """Test basic initialization."""
        card = ActionCard(
            icon="🔗",
            title="Test Card",
            description="Test description",
            primary_text="Export"
        )
        qtbot.addWidget(card)
        
        assert card.icon_label.text() == "🔗"
        assert card.title_label.text() == "Test Card"
        assert card.description_label.text() == "Test description"
    
    def test_initialization_without_description(self, qapp, qtbot):
        """Test initialization without description."""
        card = ActionCard(
            icon="🔗",
            title="Test Card",
            primary_text="Export"
        )
        qtbot.addWidget(card)
        
        assert not card.description_label.isVisible()
    
    def test_set_status(self, qapp, qtbot):
        """Test setting status text."""
        card = ActionCard(icon="🔗", title="Test")
        qtbot.addWidget(card)
        
        card.set_status("Ready", is_ready=True)
        assert "🟢" in card.status_label.text()
        assert "Ready" in card.status_label.text()
        
        card.set_status("Not ready", is_ready=False)
        assert "⚪" in card.status_label.text()
        assert "Not ready" in card.status_label.text()
    
    def test_set_status_raw(self, qapp, qtbot):
        """Test setting raw status text."""
        card = ActionCard(icon="🔗", title="Test")
        qtbot.addWidget(card)
        
        card.set_status("Custom status")
        assert card.status_label.text() == "Custom status"
    
    def test_add_action(self, qapp, qtbot):
        """Test adding actions to the card."""
        card = ActionCard(icon="🔗", title="Test")
        qtbot.addWidget(card)
        
        action = card.add_action("Update", "update", "🔄")
        
        assert action.data() == "update"
        assert not card.split_button.dropdown_button.isHidden()
    
    def test_primary_clicked_signal(self, qapp, qtbot):
        """Test that primary_clicked signal is emitted."""
        card = ActionCard(icon="🔗", title="Test", primary_text="Export")
        qtbot.addWidget(card)
        
        with qtbot.waitSignal(card.primary_clicked, timeout=1000):
            card.split_button.primary_button.click()
    
    def test_action_triggered_signal(self, qapp, qtbot):
        """Test that action_triggered signal is emitted."""
        card = ActionCard(icon="🔗", title="Test")
        qtbot.addWidget(card)
        
        action = card.add_action("Update", "update")
        
        with qtbot.waitSignal(card.action_triggered, timeout=1000) as blocker:
            action.trigger()
        
        assert blocker.args == ["update"]
    
    def test_set_enabled(self, qapp, qtbot):
        """Test enabling/disabling the card."""
        card = ActionCard(icon="🔗", title="Test")
        qtbot.addWidget(card)
        
        card.setEnabled(False)
        assert not card.split_button.isEnabled()
        
        card.setEnabled(True)
        assert card.split_button.isEnabled()
    
    def test_set_action_enabled(self, qapp, qtbot):
        """Test enabling/disabling specific actions."""
        card = ActionCard(icon="🔗", title="Test")
        qtbot.addWidget(card)
        
        card.add_action("Update", "update")
        
        card.set_action_enabled("update", False)
        
        for action in card.split_button.menu.actions():
            if action.data() == "update":
                assert not action.isEnabled()
    
    def test_hover_effect(self, qapp, qtbot):
        """Test that hover effect changes shadow."""
        from PySide6.QtCore import QPointF
        from PySide6.QtGui import QEnterEvent
        
        card = ActionCard(icon="🔗", title="Test")
        qtbot.addWidget(card)
        card.show()
        
        shadow = card.graphicsEffect()
        initial_blur = shadow.blurRadius()
        
        # Simulate enter event with proper QEnterEvent
        enter_event = QEnterEvent(QPointF(10, 10), QPointF(10, 10), QPointF(10, 10))
        card.enterEvent(enter_event)
        assert shadow.blurRadius() > initial_blur
        
        # Simulate leave event
        from PySide6.QtCore import QEvent
        leave_event = QEvent(QEvent.Leave)
        card.leaveEvent(leave_event)
        assert shadow.blurRadius() == initial_blur
    
    def test_size_constraints(self, qapp, qtbot):
        """Test that card has proper size constraints."""
        card = ActionCard(icon="🔗", title="Test")
        qtbot.addWidget(card)
        
        assert card.minimumWidth() == ActionCard.CARD_MIN_WIDTH
        assert card.maximumWidth() == ActionCard.CARD_MAX_WIDTH
        assert card.minimumHeight() == ActionCard.CARD_MIN_HEIGHT


class TestComponentsIntegration:
    """Integration tests for the new components."""
    
    def test_collapsible_section_with_action_cards(self, qapp, qtbot):
        """Test CollapsibleSection containing ActionCards."""
        from src.ui.flow_layout import FlowLayout
        
        section = CollapsibleSection("Metadaten", expanded=True)
        qtbot.addWidget(section)
        
        flow = FlowLayout(h_spacing=16, v_spacing=16)
        
        card1 = ActionCard(icon="🔗", title="URLs")
        card2 = ActionCard(icon="👥", title="Authors")
        
        flow.addWidget(card1)
        flow.addWidget(card2)
        
        section.set_content_layout(flow)
        
        # Cards should be accessible after setting layout
        assert section.is_expanded()
    
    def test_action_card_with_multiple_actions(self, qapp, qtbot):
        """Test ActionCard with multiple dropdown actions."""
        card = ActionCard(icon="🔗", title="URLs", primary_text="📥 Export")
        qtbot.addWidget(card)
        
        card.add_action("Update from CSV", "update", "🔄")
        card.add_separator()
        card.add_action("Delete all", "delete", "🗑️")
        
        # Should have 3 items in menu (2 actions + 1 separator)
        assert len(card.split_button.menu.actions()) == 3
