import json
from confluent_kafka import Producer, Consumer, KafkaError

KAFKA_BROKER = 'localhost:9092'

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def simulate_openrouter_diagnosis(original_text, research_context):
    print(f"[Diagnoser Agent] Synthesizing context via OpenRouter...")
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
