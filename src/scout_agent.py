import json
import re
from confluent_kafka import Producer, Consumer, KafkaError

KAFKA_BROKER = 'localhost:9092'

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def simulate_exa_search(query):
    print(f"[Scout Agent] Searching Exa for: {query}")
    return f"Exa Search Result for '{query}': Found similar issue in internal docs. Root cause is typically a misconfigured Auth0 audience in the prod environment."

def delivery_report(err, msg):
    if err is not None:
        print(f'[Scout Agent] Delivery failed: {err}')
    else:
        print(f'[Scout Agent] Delivered context to {msg.topic()}')

def main():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'scout_agent_group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['slack-inbound'])
    print("[Scout Agent] Listening for errors on slack-inbound...")
    
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
            text = data.get("text", "")
            
            # Simple heuristic for detecting an error or stack trace
            if "error" in text.lower() or "exception" in text.lower() or "traceback" in text.lower() or "failed" in text.lower():
                print(f"[Scout Agent] Detected issue signature in message: {text}")
                
                # Perform Exa Search
                context = simulate_exa_search(text)
                
                context_data = {
                    "channel_id": data.get("channel_id"),
                    "original_text": text,
                    "research_context": context,
                    "source": "exa_scout"
                }
                
                # Publish silently to agent-context
                producer.produce('agent-context', json.dumps(context_data).encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                
        except Exception as e:
            print(f"[Scout Agent] Error processing message: {e}")

if __name__ == "__main__":
    main()
