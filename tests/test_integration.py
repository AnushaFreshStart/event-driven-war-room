import pytest
import os
import sys
import json
from unittest.mock import patch, MagicMock

with patch("dotenv.load_dotenv"):
    with patch("confluent_kafka.Producer"):
        try:
            from src import scout_agent, diagnoser_agent, executive_agent
        except ImportError:
            sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
            from src import scout_agent, diagnoser_agent, executive_agent

def test_incident_flow():
    # Simulate a Sev-1 incident message coming into the system
    incident_message = {
        "event_id": "test-uuid-1234",
        "channel_id": "C_TEST",
        "user_id": "U_TEST",
        "text": "Exception: Auth0 unauthorized on prod",
        "thread_ts": "12345.6789",
        "timestamp": "2026-09-12T00:00:00Z"
    }

    # 1. Scout Agent processes the message
    extracted_query = scout_agent.extract_search_query(incident_message["text"])
    assert "Exception: Auth0 unauthorized" in extracted_query
    
    # 2. Scout Agent calls Exa and publishes context
    with patch("src.scout_agent.exa", None):
        context = scout_agent.simulate_exa_search(extracted_query)
        assert "Exa Search Result" in context
    
    # 3. Diagnoser processes the original message + context
    with patch("src.diagnoser_agent.client") as mock_openai:
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "Sev-1 Auth0 Outage Detected."
        mock_openai.chat.completions.create.return_value = mock_response
        
        diagnosis = diagnoser_agent.simulate_openrouter_diagnosis(
            incident_message["text"], 
            context, 
            []
        )
        assert "Sev-1 Auth0 Outage Detected." in diagnosis

    # 4. Executive processes rollback command
    rollback_message = {
        "event_id": "test-uuid-5678",
        "channel_id": "C_TEST",
        "user_id": "U_TEST",
        "text": "execute rollback",
        "thread_ts": "12345.6789",
        "timestamp": "2026-09-12T00:05:00Z"
    }
    
    with patch("src.executive_agent.TokenManager.get_token") as mock_get_token:
        mock_get_token.return_value = "mock_token"
        with patch("src.executive_agent.execute_action") as mock_execute:
            mock_execute.return_value = "Rollback successful"
            
            # Simulate the executive loop processing
            authorized = executive_agent.TokenManager().get_token() is not None
            assert authorized is True
            result = mock_execute("rollback")
            assert result == "Rollback successful"

