import os
import json
import threading
import uuid
import signal
import sys
import time
from datetime import datetime, timezone
from collections import deque
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError
from confluent_kafka import Producer, Consumer, KafkaError

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

KAFKA_BROKER = 'localhost:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})
consumer_instance = None

# Deduplication tracking
seen_msg_ids = deque(maxlen=1000)

# Graceful shutdown flag
shutdown_event = threading.Event()
consumer_thread = None

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def process_inbound_event(event, is_mention=False):
    client_msg_id = event.get("client_msg_id")
    if client_msg_id:
        if client_msg_id in seen_msg_ids:
            print(f"Skipping duplicate message: {client_msg_id}")
            return
        seen_msg_ids.append(client_msg_id)

    text = event.get("text")
    channel_id = event.get("channel")
    user_id = event.get("user")
    
    # Thread support
    thread_ts = event.get("thread_ts") or event.get("ts")
    
    if text:
        action_type = "Mentioned in" if is_mention else "Received from"
        print(f"{action_type} Slack: {text}")
        message_data = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "channel_id": channel_id,
            "user_id": user_id,
            "text": text,
            "thread_ts": thread_ts
        }
        if is_mention:
            message_data["is_mention"] = True
            
        producer.produce('slack-inbound', json.dumps(message_data).encode('utf-8'), callback=delivery_report)
        producer.poll(0)

@app.event("message")
def handle_message_events(body, logger):
    event = body.get("event", {})
    if event.get("bot_id"):
        return
    process_inbound_event(event, is_mention=False)

@app.event("app_mention")
def handle_app_mention_events(body, logger):
    event = body.get("event", {})
    process_inbound_event(event, is_mention=True)

def kafka_consumer_thread():
    global consumer_instance
    consumer_instance = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'slack_gateway_group',
        'auto.offset.reset': 'latest'
    })
    consumer_instance.subscribe(['slack-outbound'])
    print("Started Kafka consumer for slack-outbound...")
    
    while not shutdown_event.is_set():
        msg = consumer_instance.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON, sending to DLQ: {e}")
            producer.produce('dead-letter', msg.value(), callback=delivery_report)
            producer.poll(0)
            continue
            
        try:
            channel_id = data.get("channel_id")
            text = data.get("text")
            thread_ts = data.get("thread_ts")
            
            if channel_id and text:
                print(f"Publishing to Slack channel {channel_id}: {text}")
                
                # Slack rate limiting on outbound
                max_retries = 3
                for attempt in range(max_retries + 1):
                    try:
                        app.client.chat_postMessage(
                            channel=channel_id, 
                            text=text,
                            thread_ts=thread_ts
                        )
                        break
                    except SlackApiError as e:
                        if e.response.status_code == 429 and attempt < max_retries:
                            retry_after = int(e.response.headers.get('Retry-After', 1))
                            print(f"Rate limited by Slack, retrying in {retry_after} seconds...")
                            time.sleep(retry_after)
                        else:
                            raise e
        except Exception as e:
            print(f"Error processing outbound message: {e}")
            
    print("Closing Kafka consumer loop...")
    if consumer_instance:
        consumer_instance.close()

def signal_handler(sig, frame):
    print("\nShutting down gracefully...")
    shutdown_event.set()
    
    # Wait briefly for consumer thread to exit if it is not the current thread
    global consumer_thread
    if consumer_thread and threading.current_thread() != consumer_thread:
        consumer_thread.join(timeout=2.0)
        
    print("Flushing Kafka producer...")
    producer.flush()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer_thread = threading.Thread(target=kafka_consumer_thread)
    consumer_thread.start()
    
    print("Starting Slack Gateway...")
    try:
        handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
        handler.start()
    except Exception as e:
        print(f"Error in SocketModeHandler: {e}")
        signal_handler(None, None)
