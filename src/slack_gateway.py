import os
import json
import threading
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from confluent_kafka import Producer, Consumer, KafkaError

load_dotenv()

# Initialize Slack App
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

@app.event("message")
def handle_message_events(body, logger):
    event = body.get("event", {})
    # Ignore bot messages to prevent loops
    if event.get("bot_id"):
        return
        
    text = event.get("text")
    channel_id = event.get("channel")
    user_id = event.get("user")
    
    if text:
        print(f"Received from Slack: {text}")
        message_data = {
            "channel_id": channel_id,
            "user_id": user_id,
            "text": text
        }
        # Publish to Kafka
        producer.produce('slack-inbound', json.dumps(message_data).encode('utf-8'), callback=delivery_report)
        producer.poll(0)

@app.event("app_mention")
def handle_app_mention_events(body, logger):
    event = body.get("event", {})
    text = event.get("text")
    channel_id = event.get("channel")
    user_id = event.get("user")
    
    if text:
        print(f"Mentioned in Slack: {text}")
        message_data = {
            "channel_id": channel_id,
            "user_id": user_id,
            "text": text,
            "is_mention": True
        }
        producer.produce('slack-inbound', json.dumps(message_data).encode('utf-8'), callback=delivery_report)
        producer.poll(0)

def kafka_consumer_thread():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'slack_gateway_group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['slack-outbound'])
    print("Started Kafka consumer for slack-outbound...")
    
    while True:
        msg = consumer.poll(1.0)
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
            channel_id = data.get("channel_id")
            text = data.get("text")
            
            if channel_id and text:
                print(f"Publishing to Slack channel {channel_id}: {text}")
                app.client.chat_postMessage(channel=channel_id, text=text)
        except Exception as e:
            print(f"Error processing outbound message: {e}")

if __name__ == "__main__":
    # Start Kafka consumer in background thread
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    
    # Start Socket Mode Handler
    print("Starting Slack Gateway...")
    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    handler.start()
