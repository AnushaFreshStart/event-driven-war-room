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

@patch('src.executive_agent.requests.get')
@patch('src.executive_agent.requests.post')
def test_verify_auth0_permissions_real_flow_hits_auth0(mock_post, mock_get):
    """Real Auth0 mode should reach an Auth0 endpoint instead of silently succeeding."""
    mock_post_response = MagicMock()
    mock_post_response.json.return_value = {"access_token": "valid_token", "expires_in": 3600}
    mock_post.return_value = mock_post_response

    mock_get_response = MagicMock()
    mock_get_response.status_code = 200
    mock_get_response.json.return_value = [{"sub": "auth0|user-1"}]
    mock_get.return_value = mock_get_response

    import src.executive_agent as executive_agent
    executive_agent.AUTH0_DOMAIN = "tenant.auth0.com"
    executive_agent.AUTH0_CLIENT_ID = "client-id"
    executive_agent.AUTH0_CLIENT_SECRET = "client-secret"
    executive_agent.AUTH0_AUDIENCE = "https://api.example.com"

    assert executive_agent.verify_auth0_permissions("U_TEST") is True
    mock_post.assert_called_once()
    mock_get.assert_called_once()
    assert "Authorization" in mock_get.call_args.kwargs["headers"]
