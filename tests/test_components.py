import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch

# Ensure modules can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import salvage_json_items, repair_json_content, clean_content
from modules.key_manager import KeyManager

def test_json_repair():
    # Test trailing comma fix
    malformed = '{"key": "value",}'
    fixed = repair_json_content(malformed)
    assert json.loads(fixed) == {"key": "value"}

    # Test missing space in braces
    malformed = '{"a":1}{"b":2}'
    fixed = repair_json_content(malformed)
    assert '}, {' in fixed

def test_salvage_basic():
    text = """
    Here is the data:
    {"category": "NEWS", "event_summary": "Test 1", "primary_entity": "Entity 1"}
    and some noise
    {"category": "TECH", "event_summary": "Test 2", "primary_entity": "Entity 2"}
    """
    items = salvage_json_items(text)
    assert len(items) == 2
    assert items[0]['category'] == 'NEWS'
    assert items[1]['category'] == 'TECH'

def test_salvage_fragment_patching():
    # Test cutoff mid-object
    text = """
    {"category": "MARKET", "event_summary": "Complete Item", "primary_entity": "A"}
    {"category": "CRITICAL", "event_summary": "This one is cut off
    """
    items = salvage_json_items(text)
    assert len(items) == 2
    assert items[0]['event_summary'] == "Complete Item"
    assert items[1]['category'] == "CRITICAL"
    assert "[RECOVERED FRAGMENT]" in items[1]['event_summary']
    assert items[1].get('is_truncated') is True

def test_clean_content():
    # Test cleaning of lists/nulls
    assert clean_content(["  line 1  ", None, "line 2"]) == ["line 1", "line 2"]
    assert clean_content(None) == []

def test_key_manager_token_estimation():
    text = "A" * 100 # 100 chars
    # Current estimate is int(len/2.5) + 1 -> 40 + 1 = 41
    assert KeyManager.estimate_tokens(text) == 41

@patch('modules.key_manager.libsql_client.create_client_sync')
def test_key_manager_initialization(mock_create):
    # Mock database to allow KeyManager init
    mock_db = MagicMock()
    mock_create.return_value = mock_db
    
    km = KeyManager("libsql://test", "token")
    assert km.db_url == "https://test"
    assert mock_db.execute.called
