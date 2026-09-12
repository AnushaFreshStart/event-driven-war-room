import json
import os
import signal
import sys
from collections import deque, defaultdict
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer, KafkaError
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BROKER = 'localhost:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "mock-openrouter-key")
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")
if OPENROUTER_API_KEY != "mock-openrouter-key" and OPENROUTER_API_KEY:
    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)
else:
    client = None

# Global state
running = True
# Buffer for recent slack messages per channel
# channel_id -> deque of last 20 messages
slack_history = defaultdict(lambda: deque(maxlen=20))

def signal_handler(sig, frame):
    global running
    print('\n[Diagnoser Agent] Shutting down gracefully...')
    running = False

def simulate_openrouter_diagnosis(original_text, research_context, history):
    history_text = "\n".join(f"- {msg}" for msg in history) if history else "No recent history."
    
    system_prompt = (
        "You are an expert DevOps AI assistant. "
        "Your task is to analyze incident reports and background context to provide a diagnosis.\n"
        "You must output:\n"
        "1. A severity assessment (Sev-1 through Sev-4)\n"
        "2. The likely affected service\n"
        "3. Immediate mitigation steps\n"
        "4. A recommendation on whether a rollback is needed."
    )
    
    prompt = (
        f"An incident occurred: {original_text}\n\n"
        f"Recent conversation history in this channel:\n{history_text}\n\n"
        f"Here is the background research:\n{research_context}\n\n"
        f"Please provide a professional incident diagnosis as per instructions."
    )
    
    if client:
        print(f"[Diagnoser Agent] Synthesizing REAL context via OpenRouter using model {OPENROUTER_MODEL}...")
        try:
            models_to_try = [OPENROUTER_MODEL]
            if OPENROUTER_MODEL != "openai/gpt-4o-mini":
                models_to_try.append("openai/gpt-4o-mini")
            last_error = None
            for model_name in models_to_try:
                try:
                    response = client.chat.completions.create(
                        model=model_name,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": prompt}
                        ]
                    )
                    return f":mag: *AI Diagnosis*\n{response.choices[0].message.content}"
                except Exception as e:
                    last_error = e
                    print(f"[Diagnoser Agent] Model {model_name} failed: {e}")
                    if "404" not in str(e):
                        break
            return f"OpenRouter API failed: {last_error}"
        except Exception as e:
            return f"OpenRouter API failed: {e}"
    else:
        print(f"[Diagnoser Agent] Mock Synthesizing context via OpenRouter...")
        return (f":mag: *Automated Diagnosis*\n"
                f"> Based on the error `{original_text}` and Exa background research:\n"
                f"> {research_context}\n\n"
                f"*Severity*: Sev-2\n"
                f"*Affected Service*: Unknown (Likely Auth0)\n"
                f"*Mitigation Steps*: Inspect recent deployments.\n"
                f"*Recommendation*: Verify the production Auth0 audience. If misconfigured, please run `@ExecutiveAgent execute rollback`.")

def delivery_report(err, msg):
    if err is not None:
        print(f'[Diagnoser Agent] Delivery failed: {err}')
    else:
        print(f'[Diagnoser Agent] Delivered message to {msg.topic()}')

def main():
    global running
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'diagnoser_agent_group',
        'auto.offset.reset': 'latest'
    })
    
    # Subscribe to both slack-inbound and agent-context
    consumer.subscribe(['slack-inbound', 'agent-context'])
    print("[Diagnoser Agent] Listening on slack-inbound and agent-context...")
    
    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break
                
        topic = msg.topic()
        raw_value = msg.value()
        
        try:
            data = json.loads(raw_value.decode('utf-8'))
        except Exception as e:
            print(f"[Diagnoser Agent] Failed to parse JSON, routing to dead-letter: {e}")
            producer.produce('dead-letter', raw_value, callback=delivery_report)
            producer.poll(0)
            continue
            
        try:
            if topic == 'slack-inbound':
                channel_id = data.get("channel_id")
                text = data.get("text")
                if channel_id and text:
                    slack_history[channel_id].append(text)
            
            elif topic == 'agent-context':
                channel_id = data.get("channel_id")
                original_text = data.get("original_text")
                research_context = data.get("research_context")
                
                # Propagate correlation fields
                event_id = data.get("event_id")
                thread_ts = data.get("thread_ts")
                timestamp = data.get("timestamp")
                
                print(f"[Diagnoser Agent] Received context for channel {channel_id}")
                
                # Retrieve history for the channel
                history = list(slack_history[channel_id]) if channel_id in slack_history else []
                
                diagnosis = simulate_openrouter_diagnosis(original_text, research_context, history)
                
                response_data = {
                    "channel_id": channel_id,
                    "text": diagnosis,
                }
                
                if event_id:
                    response_data["event_id"] = event_id
                if thread_ts:
                    response_data["thread_ts"] = thread_ts
                if timestamp:
                    response_data["timestamp"] = timestamp
                
                producer.produce('slack-outbound', json.dumps(response_data).encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                
        except Exception as e:
            print(f"[Diagnoser Agent] Error processing message: {e}")

    # Graceful shutdown cleanup
    consumer.close()
    producer.flush()
    print("[Diagnoser Agent] Shutdown complete.")

if __name__ == "__main__":
    main()
