import pytest
import os
import sys
from unittest.mock import patch, MagicMock

with patch("dotenv.load_dotenv"):
    with patch("confluent_kafka.Producer"):
        with patch("slack_bolt.App"):
            try:
                from src import slack_gateway
            except ImportError:
                sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
                from src import slack_gateway

def test_gateway_deduplication():
    # Setup mock event
    event_body = {
        "event": {
            "client_msg_id": "msg-123",
            "text": "Hello",
            "channel": "C1",
            "user": "U1",
            "ts": "1234.56"
        }
    }
    
    # Verify new message is not duplicate
    slack_gateway.seen_msg_ids.clear()
    
    # Process first time
    slack_gateway.process_inbound_event(event_body["event"], is_mention=False)
    assert "msg-123" in slack_gateway.seen_msg_ids
    
    # Try processing duplicate
    with patch.object(slack_gateway.producer, 'produce') as mock_produce:
        slack_gateway.process_inbound_event(event_body["event"], is_mention=False)
        # produce should NOT have been called because it's a duplicate
        mock_produce.assert_not_called()
