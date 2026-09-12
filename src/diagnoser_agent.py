import json
import os
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer, KafkaError
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BROKER = 'localhost:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "mock-openrouter-key")
if OPENROUTER_API_KEY != "mock-openrouter-key" and OPENROUTER_API_KEY:
    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)
else:
    client = None

def simulate_openrouter_diagnosis(original_text, research_context):
    if client:
        print(f"[Diagnoser Agent] Synthesizing REAL context via OpenRouter...")
        try:
            prompt = f"An incident occurred: {original_text}\n\nHere is the background research:\n{research_context}\n\nPlease provide a brief, professional incident diagnosis and recommend a fix (e.g. executing a rollback if necessary)."
            response = client.chat.completions.create(
                model="anthropic/claude-3.5-sonnet",
                messages=[
                    {"role": "system", "content": "You are an expert DevOps AI assistant."},
                    {"role": "user", "content": prompt}
                ]
            )
            return f":mag: *AI Diagnosis*\n{response.choices[0].message.content}"
        except Exception as e:
            return f"OpenRouter API failed: {e}"
    else:
        print(f"[Diagnoser Agent] Mock Synthesizing context via OpenRouter...")
        return (f":mag: *Automated Diagnosis*\n"
                f"> Based on the error `{original_text}` and Exa background research:\n"
                f"> {research_context}\n\n"
                f"*Recommendation:* Verify the production Auth0 audience. If misconfigured, please run `@ExecutiveAgent execute rollback`.")

def delivery_report(err, msg):
    if err is not None:
        print(f'[Diagnoser Agent] Delivery failed: {err}')
    else:
        print(f'[Diagnoser Agent] Delivered diagnosis to {msg.topic()}')

def main():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'diagnoser_agent_group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['agent-context'])
    print("[Diagnoser Agent] Listening for context events on agent-context...")
    
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
            original_text = data.get("original_text")
            research_context = data.get("research_context")
            
            print(f"[Diagnoser Agent] Received context for channel {channel_id}")
            
            # Synthesize via OpenRouter
            diagnosis = simulate_openrouter_diagnosis(original_text, research_context)
            
            response_data = {
                "channel_id": channel_id,
                "text": diagnosis
            }
            
            # Publish to slack-outbound
            producer.produce('slack-outbound', json.dumps(response_data).encode('utf-8'), callback=delivery_report)
            producer.poll(0)
            
        except Exception as e:
            print(f"[Diagnoser Agent] Error processing message: {e}")

if __name__ == "__main__":
    main()
