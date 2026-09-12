import json
import os
import re
import signal
import sys
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

from confluent_kafka import Producer, Consumer, KafkaError
from exa_py import Exa

KAFKA_BROKER = 'localhost:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

EXA_API_KEY = os.environ.get("EXA_API_KEY", "mock-exa-key")
if EXA_API_KEY != "mock-exa-key" and EXA_API_KEY:
    exa = Exa(EXA_API_KEY)
else:
    exa = None

def extract_search_query(text):
    error_patterns = [
        r'(?i)(Exception|Error|Traceback|Panic|Fatal|Warning):?\s*(.*)',
        r'(?i)(HTTP\s*status\s*code\s*|Status\s*|Code\s*)([45]\d\d)',
        r'\b([45]\d\d)\b'
    ]
    
    for pattern in error_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0).strip()
    
    return text[:200]

def simulate_exa_search(query):
    if exa:
        print(f"[Scout Agent] Performing REAL Exa Search for: {query}")
        try:
            results = exa.search_and_contents(query, num_results=2, use_autoprompt=True)
            context = "\n".join([f"- {r.title}: {r.text[:300]}..." for r in results.results])
            return f"Exa Search Results for '{query}':\n{context}"
        except Exception as e:
            return f"Exa Search failed: {e}"
    else:
        print(f"[Scout Agent] Mock Searching Exa for: {query}")
        return f"Exa Search Result for '{query}': Found similar issue in internal docs. Root cause is typically a misconfigured Auth0 audience in the prod environment."

def delivery_report(err, msg):
    if err is not None:
        print(f'[Scout Agent] Delivery failed: {err}')
    else:
        print(f'[Scout Agent] Delivered context to {msg.topic()}')

consumer = None

def shutdown_handler(sig, frame):
    print("[Scout Agent] Shutting down...")
    if consumer:
        consumer.close()
    producer.flush()
    sys.exit(0)

def main():
    global consumer
    
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'scout_agent_group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['slack-inbound'])
    print("[Scout Agent] Listening for errors on slack-inbound...")
    
    trigger_keywords = [
        "error", "exception", "traceback", "failed",
        "timeout", "crash", "outage", "down", 
        "500", "502", "503", "504", "oom", "killed", "panic"
    ]
    
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            
            raw_value = msg.value().decode('utf-8')
            try:
                data = json.loads(raw_value)
            except json.JSONDecodeError:
                print("[Scout Agent] JSON decode failed, sending to dead-letter")
                producer.produce('dead-letter', msg.value(), callback=delivery_report)
                producer.poll(0)
                continue
                
            text = data.get("text", "")
            
            if any(keyword in text.lower() for keyword in trigger_keywords):
                print(f"[Scout Agent] Detected issue signature in message: {text}")
                
                query = extract_search_query(text)
                context = simulate_exa_search(query)
                
                context_data = {
                    "channel_id": data.get("channel_id"),
                    "original_text": text,
                    "research_context": context,
                    "source": "exa_scout",
                    "event_id": data.get("event_id"),
                    "thread_ts": data.get("thread_ts"),
                    "timestamp": data.get("timestamp")
                }
                producer.produce('agent-context', json.dumps(context_data).encode('utf-8'), callback=delivery_report)
                producer.poll(0)
        except Exception as e:
            print(f"[Scout Agent] Error processing message: {e}")

if __name__ == '__main__':
    main()
