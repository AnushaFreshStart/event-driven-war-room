import sys
import pytest
from unittest.mock import MagicMock, patch
import requests

# 2. Mock/patch dotenv.load_dotenv and confluent_kafka.Producer/Consumer before importing src.executive_agent
mock_dotenv = MagicMock()
sys.modules['dotenv'] = mock_dotenv

mock_kafka = MagicMock()
mock_kafka.Producer = MagicMock()
mock_kafka.Consumer = MagicMock()
sys.modules['confluent_kafka'] = mock_kafka

# Now import the module to test
from src.executive_agent import TokenManager, execute_action

def test_execute_rollback():
    """1. Test execute_action('rollback')"""
    with patch('src.executive_agent.time.sleep') as mock_sleep:
        result = execute_action("rollback")
        assert "Rollback successful" in result
        mock_sleep.assert_called_once_with(2)

@patch('src.executive_agent.requests.post')
def test_token_manager_get_token_success(mock_post):
    """3. Patch requests.post to test the token fetching logic (success)"""
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "valid_token", "expires_in": 3600}
    mock_post.return_value = mock_response
    
    token_mgr = TokenManager()
    token = token_mgr.get_token()
    
    assert token == "valid_token"
    mock_post.assert_called_once()
    assert token_mgr._token == "valid_token"

@patch('src.executive_agent.requests.post')
def test_token_manager_get_token_failure_401(mock_post):
    """3. Patch requests.post to test the token fetching logic (failure 401)"""
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("401 Client Error: Unauthorized")
    mock_post.return_value = mock_response
    
    token_mgr = TokenManager()
    token = token_mgr.get_token()
    
    assert token is None
    mock_post.assert_called_once()
    assert token_mgr._token is None
