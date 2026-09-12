import pytest
import os
import sys
from unittest.mock import patch, MagicMock

with patch("dotenv.load_dotenv"):
    with patch("confluent_kafka.Producer"):
        try:
            from src.scout_agent import extract_search_query, simulate_exa_search
        except ImportError:
            sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
            from src.scout_agent import extract_search_query, simulate_exa_search

def test_extract_search_query_stack_trace():
    prompt = "Error: NullReferenceException at Line 42 in UserService.cs"
    query = extract_search_query(prompt)
    assert isinstance(query, str)
    assert len(query) > 0

def test_extract_search_query_http_500():
    prompt = "HTTP 500 Internal Server Error when calling /api/v1/users"
    query = extract_search_query(prompt)
    assert isinstance(query, str)
    assert len(query) > 0

def test_extract_search_query_simple_sentence():
    prompt = "How to fix a memory leak in Node.js?"
    query = extract_search_query(prompt)
    assert isinstance(query, str)
    assert len(query) > 0

@patch("src.scout_agent.exa", None)
def test_simulate_exa_search_mock_key():
    results = simulate_exa_search("NullReferenceException")
    assert isinstance(results, str)
    assert "Exa Search Result for 'NullReferenceException'" in results
    assert "Found similar issue in internal docs" in results

@patch("src.scout_agent.exa")
def test_simulate_exa_search_real_key(mock_exa):
    mock_response = MagicMock()
    mock_result = MagicMock()
    mock_result.title = "How to fix NullReferenceException"
    mock_result.text = "This is how you fix the exception..."
    
    mock_response.results = [mock_result]
    mock_exa.search_and_contents.return_value = mock_response
    
    results = simulate_exa_search("NullReferenceException")
    
    assert isinstance(results, str)
    assert "Exa Search Results for" in results
    assert "How to fix NullReferenceException" in results
    assert "This is how you fix the exception" in results
