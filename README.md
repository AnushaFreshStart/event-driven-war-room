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
   Add your keys to the `.env` file:
   - `SLACK_APP_TOKEN`
   - `SLACK_BOT_TOKEN`
   - `EXA_API_KEY`
   - `OPENROUTER_API_KEY`

4. **Run the Microservices (in separate terminals):**
   ```bash
   python slack_gateway.py
   python scout_agent.py
   python diagnoser_agent.py
   python executive_agent.py
   ```

## Documentation
- See [ARCHITECTURE.md](ARCHITECTURE.md) for system diagrams.
- See [AGENT_FLOWS.md](AGENT_FLOWS.md) for internal agent logic and event contracts.
