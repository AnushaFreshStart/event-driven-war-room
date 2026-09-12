# The Event-Driven War Room

A real-time, multi-agent incident response system integrated with Slack.

## The Problem
During critical incidents (e.g., a Sev-1 server outage or a cybersecurity breach), engineering teams gather in a Slack "War Room." The environment is chaotic. If teams try to use standard AI chatbots to help, they run into two problems:
1. **Rigidity:** Humans must stop debugging to explicitly `@mention` and prompt the AI.
2. **Synchronous Bottlenecks:** If one agent is tasked with searching logs, and another with checking documentation, they block each other. It’s too slow for a real-time outage.

## The Solution
Instead of rigid, 1-on-1 chat interfaces, this project implements the **"Blackboard Pattern"** via a Kafka/Redpanda event stream. 

Multiple specialized AI agents (Scout, Diagnoser, Executive) asynchronously listen to the Slack conversation, perform research using **Exa**, synthesize solutions via **OpenRouter**, and execute secure commands via **Auth0**—all without blocking each other.

To ensure the high-stakes Executive Agent performs safely, we used the **ToolGrad** methodology (from Google Research) to synthetically generate successful tool-chains and fine-tune its capabilities.

## Setup Instructions

1. **Start the Redpanda (Kafka) Broker:**
   ```bash
   docker-compose up -d
   ```

2. **Set up the Python Environment:**
   ```bash
   python -m venv .venv
   .\.venv\Scripts\activate
   pip install -r requirements.txt # (or install confluent-kafka slack_bolt exa_py openai httpx pydantic)
   ```

3. **Configure Environment Variables:**
   Add your keys to the `.env` file at the root of the project:
   - `SLACK_APP_TOKEN=xapp-...`
   - `SLACK_BOT_TOKEN=xoxb-...`
   - `EXA_API_KEY=your-key` (Optional: leave as `mock-exa-key` to use the simulated responses for testing)
   - `OPENROUTER_API_KEY=your-key` (Optional: leave as `mock-openrouter-key` to use the simulated responses)
   - `AUTH0_DOMAIN=your-tenant.auth0.com` (Optional: leave as `mock-auth0-domain` to bypass)
   - `AUTH0_CLIENT_ID=your-client-id`
   - `AUTH0_CLIENT_SECRET=your-client-secret`
   - `AUTH0_AUDIENCE=your-api-audience`

4. **Run the Microservices (in separate terminals):**
   ```bash
   python slack_gateway.py
   python scout_agent.py
   python diagnoser_agent.py
   python executive_agent.py
   ```

## Documentation
- See [API_SETUP.md](API_SETUP.md) for step-by-step instructions on acquiring Exa, OpenRouter, and Auth0 API keys.
- See [ARCHITECTURE.md](ARCHITECTURE.md) for system diagrams.
- See [AGENT_FLOWS.md](AGENT_FLOWS.md) for internal agent logic and event contracts.

## How the topics were created

I am showing the exact Kafka command and the reason it works in this project so you can recreate or verify it on your own.

This project uses Redpanda, which is Kafka-compatible, and the topic creation was done with the `rpk` CLI inside the running Docker container:

`powershell
docker exec redpanda rpk topic create slack-inbound slack-outbound agent-context --brokers localhost:9092 --if-not-exists
`

### What this does
- `docker exec redpanda ...` → runs a command inside the running Redpanda container
- `rpk topic create ...` → creates Kafka topics
- `slack-inbound slack-outbound agent-context` → the three topics used by the app
- `--brokers localhost:9092` → connects to the Kafka endpoint exposed by Redpanda
- `--if-not-exists` → avoids failing if the topics already exist

### Why it was needed
Each agent subscribes to a Kafka topic at startup. If the topic is missing, the consumer gets:

`	ext
KafkaError{code=UNKNOWN_TOPIC_OR_PART,...}
`

That is exactly why the startup error happens if the topics don't exist.

### Verify the topics
You can check them with:

`powershell
docker exec redpanda rpk topic list --brokers localhost:9092
`

Or inspect one topic:

`powershell
docker exec redpanda rpk topic describe slack-inbound --brokers localhost:9092
`

### In this repo
The broker itself is started from `docker-compose.yml`, and the topics are automatically created in `start_all.bat` before the Python services launch using the exact pattern the app expects.

### Run tests
.\.venv\Scripts\python.exe -m pip install -r requirements.txt; .\.venv\Scripts\python.exe -m pytest -q
