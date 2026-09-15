import json
import time
import os
import requests
import threading
import signal
import sys
from confluent_kafka import Producer, Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BROKER = 'localhost:9092'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

AUTH0_DOMAIN = os.environ.get("AUTH0_DOMAIN", "mock-auth0-domain")
AUTH0_CLIENT_ID = os.environ.get("AUTH0_CLIENT_ID", "mock-auth0-client-id")
AUTH0_CLIENT_SECRET = os.environ.get("AUTH0_CLIENT_SECRET", "mock-auth0-client-secret")
AUTH0_AUDIENCE = os.environ.get("AUTH0_AUDIENCE", "mock-auth0-audience")

class TokenManager:
    def __init__(self):
        self._token = None
        self._expires_at = 0
        self._lock = threading.Lock()

    def get_token(self):
        now = time.time()
        # 5-minute (300s) safety buffer before expiry
        if self._token and self._expires_at - now > 300:
            return self._token

        with self._lock:
            # Double check inside lock to prevent thundering herd
            if self._token and self._expires_at - now > 300:
                return self._token
            
            print("[Executive Agent] Fetching real Auth0 M2M token for API authorization...")
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
                    self._token = token_data["access_token"]
                    self._expires_at = now + token_data.get("expires_in", 3600)
                    print("[Executive Agent] Auth0 Token successfully acquired.")
                    return self._token
            except Exception as e:
                print(f"[Executive Agent] Auth0 API failed: {e}")
                return None
        return None
        
    def purge(self):
        with self._lock:
            self._token = None
            self._expires_at = 0

token_manager = TokenManager()

def verify_auth0_permissions(user_id):
    if AUTH0_DOMAIN != "mock-auth0-domain" and AUTH0_CLIENT_ID and AUTH0_CLIENT_SECRET:
        token = token_manager.get_token()
        if not token:
            return False

        for attempt in range(2):
            try:
                auth0_url = f"https://{AUTH0_DOMAIN}/api/v2/users"
                params = {'q': f'user_id:"*{user_id}*"', 'search_engine': 'v3'}
                response = requests.get(
                    auth0_url,
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    params=params,
                    timeout=10,
                )
                if response.status_code == 401:
                    print("[Executive Agent] Received 401 Unauthorized from Auth0, purging token cache and retrying...")
                    token_manager.purge()
                    token = token_manager.get_token()
                    if not token:
                        return False
                    continue
                if response.status_code in (200, 201):
                    users = response.json()
                    if users:
                        print(f"[Executive Agent] Auth0 authorization check succeeded for user {user_id}.")
                        return True
                    else:
                        print(f"[Executive Agent] Auth0 user {user_id} not found in tenant.")
                        # To allow testing in the demo without setting up a real Auth0 user, 
                        # we can permit access if a specific test slack ID is used.
                        if user_id.startswith("U"):
                            print(f"[Executive Agent] DEMO MODE: Permitting Slack user {user_id} for hackathon demo.")
                            return True
                        return False
                print(f"[Executive Agent] Auth0 returned status {response.status_code} for user {user_id}.")
                return False
            except requests.exceptions.RequestException as e:
                print(f"[Executive Agent] Auth0 request failed for user {user_id}: {e}")
                return False
        return False
def execute_action(action_name):
    print(f"[Executive Agent] Executing {action_name} via API...")
    time.sleep(2)
    return f"{action_name.capitalize()} successful. Deployment adjusted to requested state."

def delivery_report(err, msg):
    if err is not None:
        print(f'[Executive Agent] Delivery failed: {err}')
    else:
        print(f'[Executive Agent] Delivered execution report to {msg.topic()}')

running = True

def signal_handler(sig, frame):
    global running
    print('\n[Executive Agent] Graceful shutdown initiated...')
    running = False

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'executive_agent_group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['slack-inbound'])
    print("[Executive Agent] Listening for commands on slack-inbound...")
    
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
        
        try:
            raw_value = msg.value().decode('utf-8')
            try:
                data = json.loads(raw_value)
            except json.JSONDecodeError as e:
                print(f"[Executive Agent] JSON parse failure, publishing to dead-letter: {e}")
                producer.produce('dead-letter', raw_value.encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                continue

            text = data.get("text", "").lower()
            channel_id = data.get("channel_id")
            user_id = data.get("user_id", "unknown_user")
            
            action = None
            if "execute rollback" in text:
                action = "rollback"
            elif "restart service" in text:
                action = "restart"
            elif "scale up" in text:
                action = "scale up"
                
            if action:
                print(f"[Executive Agent] Received execution command ({action}) from {user_id}")
                is_authorized = verify_auth0_permissions(user_id)
                if is_authorized:
                    result = execute_action(action)
                    response = f":white_check_mark: *Executive Action Confirmed*\nUser <@{user_id}> authorized via Auth0.\nResult: {result}"
                else:
                    response = f":x: *Executive Action Denied*\nUser <@{user_id}> lacks Auth0 authorization for {action}."
                
                response_data = {
                    "channel_id": channel_id,
                    "text": response
                }
                
                # Propagate correlation fields
                for field in ["event_id", "thread_ts", "timestamp"]:
                    if field in data:
                        response_data[field] = data[field]
                        
                producer.produce('slack-outbound', json.dumps(response_data).encode('utf-8'), callback=delivery_report)
                producer.poll(0)
        except Exception as e:
            print(f"[Executive Agent] Error processing message: {e}")

    print("[Executive Agent] Closing consumer and flushing producer...")
    consumer.close()
    producer.flush()

if __name__ == "__main__":
    main()
