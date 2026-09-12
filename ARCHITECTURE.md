# The Event-Driven War Room Architecture

This document outlines the system architecture and event-driven data flow, demonstrating the principles from the **Confluent "Guide to Event-Driven Design for Agents"** and the **ToolGrad Google Research** paper.

## 1. System Architecture (The Blackboard Pattern)

Instead of agents communicating via rigid, synchronous API calls, this system uses the **Blackboard Pattern**. The Redpanda (Kafka) broker acts as the central "Blackboard" where agents publish and subscribe to events asynchronously. 

```mermaid
graph TD
    subgraph "External Interfaces"
        Slack["Slack Workspace (War Room)"]
        Exa["Exa Search API (Starter Kit)"]
        OpenRouter["OpenRouter API (Starter Kit)"]
        Auth0["Auth0 (Starter Kit)"]
    end

    subgraph "Event-Driven Backbone"
        Gateway["Slack Gateway (slack_gateway.py)"]
        Redpanda[("Redpanda Broker (Kafka)")]
        TopicIn["Topic: slack-inbound"]
        TopicContext["Topic: agent-context"]
        TopicOut["Topic: slack-outbound"]
    end

    subgraph "Agent Swarm (Python Microservices)"
        Scout["Scout Agent (scout_agent.py)"]
        Diagnoser["Diagnoser Agent (diagnoser_agent.py)"]
        Executive["Executive Agent (executive_agent.py)"]
    end

    %% Gateway Connections
    Slack <-->|SocketMode| Gateway
    Gateway -->|Publish| TopicIn
    Gateway <---|Consume| TopicOut

    %% Redpanda Topics
    Redpanda --- TopicIn
    Redpanda --- TopicContext
    Redpanda --- TopicOut

    %% Agent Connections
    TopicIn --->|Consume| Scout
    Scout --->|Publish| TopicContext
    Scout <.->|Search| Exa

    TopicIn --->|Consume| Diagnoser
    TopicContext --->|Consume| Diagnoser
    Diagnoser --->|Publish| TopicOut
    Diagnoser <.->|Synthesize| OpenRouter

    TopicIn --->|Consume| Executive
    Executive --->|Publish| TopicOut
    Executive <.->|Verify Permissions| Auth0
```

## 2. Event Sequence Diagram (Incident Resolution)

This sequence diagram illustrates how the loosely coupled agents interact during a Sev-1 outage. Notice how the Scout and Diagnoser agents act autonomously based on the event stream without requiring explicit `@mentions` from the human engineers.

```mermaid
sequenceDiagram
    participant H as Human (Slack)
    participant G as Slack Gateway
    participant K as Redpanda Broker
    participant S as Scout Agent
    participant D as Diagnoser Agent
    participant E as Executive Agent

    %% Initial Alert
    H->>G: "We have an exception: Auth0 unauthorized on prod"
    G->>K: Publish to `slack-inbound`
    
    %% Parallel Processing
    par Asynchronous Event Consumption
        K-->>S: Consume `slack-inbound`
        K-->>D: Consume `slack-inbound`
        K-->>E: Consume `slack-inbound` (Ignored, not a command)
    end
    
    %% Scout Action
    Note over S: Detects error signature
    S->>S: Search Exa for similar issues/docs
    S->>K: Publish findings to `agent-context`
    
    %% Diagnoser Action
    K-->>D: Consume `agent-context`
    Note over D: Synthesizes original message + Exa context via OpenRouter
    D->>K: Publish Diagnosis to `slack-outbound`
    
    %% Gateway Action
    K-->>G: Consume `slack-outbound`
    G->>H: Slack Post: "Diagnosis: Check Prod Audience. Say 'execute rollback' to revert."
    
    %% Executive Action
    H->>G: "execute rollback"
    G->>K: Publish to `slack-inbound`
    
    K-->>E: Consume `slack-inbound`
    Note over E: Detects actionable command
    E->>E: Verify Auth0 permissions for Slack User
    E->>E: Execute Rollback Tool
    E->>K: Publish success to `slack-outbound`
    
    K-->>G: Consume `slack-outbound`
    G->>H: Slack Post: "Rollback successful."
```

## 3. ToolGrad Data Generation Flow

The **Executive Agent** possesses dangerous capabilities (like rolling back a production server). To ensure it is reliable, we implement the methodology from the **ToolGrad** Google Research paper. 

Rather than hoping the agent figures out the tool, we generate a dataset of 100% successful, verified tool-execution chains *first*, and then map them to synthetic queries. This dataset can be used to fine-tune a private local model (e.g., via Mozilla.ai llamafile) specifically for the Executive Agent.

```mermaid
flowchart TD
    Start([Start Dataset Generation]) --> A[Extract Tool Schemas <br> e.g. RollbackAPI, Auth0Verify]
    A --> B{Textual Gradients via LLM}
    B --> C[Execute Tool Chain (Answer First)]
    C -->|Failure| B
    C -->|Success| D[Generate Synthetic User Query <br> e.g. 'Rollback prod server']
    D --> E[Pair Query with Successful Tool Chain]
    E --> F[Save to toolgrad_dataset.jsonl]
    F --> G([Ready for Mozilla.ai Fine-Tuning])

    style B stroke:#f66,stroke-width:2px,stroke-dasharray: 5 5
    style G fill:#9f9,stroke:#333,stroke-width:2px
```
