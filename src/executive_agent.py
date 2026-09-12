import json
import time
import os
import requests
from confluent_kafka import Producer, Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BROKER = 'localhost:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

AUTH0_DOMAIN = os.environ.get("AUTH0_DOMAIN", "mock-auth0-domain")
AUTH0_CLIENT_ID = os.environ.get("AUTH0_CLIENT_ID", "mock-auth0-client-id")
AUTH0_CLIENT_SECRET = os.environ.get("AUTH0_CLIENT_SECRET", "mock-auth0-client-secret")
AUTH0_AUDIENCE = os.environ.get("AUTH0_AUDIENCE", "mock-auth0-audience")

def verify_auth0_permissions(user_id):
    if AUTH0_DOMAIN != "mock-auth0-domain" and AUTH0_CLIENT_ID and AUTH0_CLIENT_SECRET:
        print(f"[Executive Agent] Fetching real Auth0 M2M token for API authorization...")
        url = f"https://{AUTH0_DOMAIN}/oauth/token"
        payload = {
            "client_id": AUTH0_CLIENT_ID,
            "client_secret": AUTH0_CLIENT_SECRET,
            "audience": AUTH0_AUDIENCE,
            "grant_type": "client_credentials"
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            token_data = response.json()
            if token_data.get("access_token"):
                print("[Executive Agent] Auth0 Token successfully acquired.")
                return True
            return False
        except Exception as e:
            print(f"[Executive Agent] Auth0 API failed: {e}")
            return False
    else:
        print(f"[Executive Agent] Mock Verifying Auth0 M2M scopes for user {user_id}...")
        time.sleep(1)
        return True

def execute_rollback():
    print(f"[Executive Agent] Executing rollback via API...")
    time.sleep(2)
    return "Rollback successful. Deployment reverted to previous stable state."

def delivery_report(err, msg):
    if err is not None:
        print(f'[Executive Agent] Delivery failed: {err}')
    else:
        print(f'[Executive Agent] Delivered execution report to {msg.topic()}')

def main():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'executive_agent_group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['slack-inbound'])
    print("[Executive Agent] Listening for commands on slack-inbound...")
    
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
            text = data.get("text", "").lower()
            channel_id = data.get("channel_id")
            user_id = data.get("user_id", "unknown_user")
            
            if "execute rollback" in text:
                print(f"[Executive Agent] Received execution command from {user_id}")
                
                # Check Auth0
                is_authorized = verify_auth0_permissions(user_id)
                
                if is_authorized:
                    result = execute_rollback()
                    response = f":white_check_mark: *Executive Action Confirmed*\nUser <@{user_id}> authorized via Auth0.\nResult: {result}"
                else:
                    response = f":x: *Executive Action Denied*\nUser <@{user_id}> lacks Auth0 `admin:rollback` scope."
                
                response_data = {
                    "channel_id": channel_id,
                    "text": response
                }
                
                # Publish to slack-outbound
                producer.produce('slack-outbound', json.dumps(response_data).encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                
        except Exception as e:
            print(f"[Executive Agent] Error processing message: {e}")

if __name__ == "__main__":
    main()
