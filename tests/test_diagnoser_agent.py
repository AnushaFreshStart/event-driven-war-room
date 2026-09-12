import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# Mock dotenv and confluent_kafka before importing src.diagnoser_agent
mock_dotenv = patch('dotenv.load_dotenv').start()
sys.modules['confluent_kafka'] = MagicMock()

# Add parent directory to sys.path to allow importing src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import src.diagnoser_agent as diagnoser_agent
from src.diagnoser_agent import simulate_openrouter_diagnosis

def test_simulate_openrouter_diagnosis_standard(mocker):
    """Test standard diagnosis output."""
    mock_client = mocker.patch.object(diagnoser_agent, 'client')
    mock_response = MagicMock()
    mock_response.choices[0].message.content = "Database connection timeout detected."
    mock_client.chat.completions.create.return_value = mock_response

    result = simulate_openrouter_diagnosis("Error: Cannot connect to database", "Exa Results", [])
    
    assert result == ":mag: *AI Diagnosis*\nDatabase connection timeout detected."
    mock_client.chat.completions.create.assert_called_once()

def test_simulate_openrouter_diagnosis_empty_input(mocker):
    """Test diagnosis with empty input string."""
    mock_client = mocker.patch.object(diagnoser_agent, 'client')
    mock_response = MagicMock()
    mock_response.choices[0].message.content = "Please provide an issue description."
    mock_client.chat.completions.create.return_value = mock_response

    result = simulate_openrouter_diagnosis("", "", [])
    
    assert result == ":mag: *AI Diagnosis*\nPlease provide an issue description."
    mock_client.chat.completions.create.assert_called_once()
