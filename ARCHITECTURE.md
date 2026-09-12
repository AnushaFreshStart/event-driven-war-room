# The Event-Driven War Room Architecture

This document outlines the system architecture and event-driven data flow, demonstrating the principles from the **Confluent "Guide to Event-Driven Design for Agents"** and the **ToolGrad Google Research** paper.

## 1. System Architecture (The Blackboard Pattern)

Instead of agents communicating via rigid, synchronous API calls, this system uses the **Blackboard Pattern**. The Redpanda (Kafka) broker acts as the central "Blackboard" where agents publish and subscribe to events asynchronously. 

```mermaid
graph TD
    subgraph "External Interfaces"
        Slack["Slack Workspace (War Room)"]
        Exa["Exa Search API"]
        OpenRouter["OpenRouter API"]
        Auth0["Auth0 M2M"]
    end

    subgraph "Event-Driven Backbone (Redpanda)"
        Gateway["Slack Gateway<br>(slack_gateway.py)<br>• Dedup · Correlation IDs<br>• Rate Limiting · Threading"]
        TopicIn["Topic: slack-inbound"]
        TopicContext["Topic: agent-context"]
        TopicOut["Topic: slack-outbound"]
        TopicDLQ["Topic: dead-letter"]
    end

    subgraph "Agent Swarm (Python Microservices)"
        Scout["Scout Agent<br>(scout_agent.py)<br>• Keyword Extraction<br>• Graceful Shutdown"]
        Diagnoser["Diagnoser Agent<br>(diagnoser_agent.py)<br>• Conversation Buffer<br>• Severity Assessment"]
        Executive["Executive Agent<br>(executive_agent.py)<br>• Token Caching<br>• Multi-Command"]
    end

    %% Gateway Connections
    Slack <-->|SocketMode + Threading| Gateway
    Gateway -->|Publish with event_id| TopicIn
    Gateway <---|Consume with retry| TopicOut

    %% Agent Connections
    TopicIn --->|Consume| Scout
    Scout --->|Publish| TopicContext
    Scout <.->|Extracted Query| Exa

    TopicIn --->|Consume + Buffer| Diagnoser
    TopicContext --->|Consume| Diagnoser
    Diagnoser --->|Publish| TopicOut
    Diagnoser <.->|Enriched Prompt| OpenRouter

    TopicIn --->|Consume| Executive
    Executive --->|Publish| TopicOut
    Executive <.->|Cached M2M Token| Auth0

    %% DLQ
    Scout -.->|Parse Errors| TopicDLQ
    Diagnoser -.->|Parse Errors| TopicDLQ
    Executive -.->|Parse Errors| TopicDLQ
    Gateway -.->|Parse Errors| TopicDLQ
```

## 2. Event Sequence Diagram (Incident Resolution)

This sequence diagram illustrates how the loosely coupled agents interact during a Sev-1 outage. All responses are posted **in-thread** to keep the main channel clean.

```mermaid
sequenceDiagram
    participant H as Human (Slack)
    participant G as Slack Gateway
    participant K as Redpanda Broker
    participant S as Scout Agent
    participant D as Diagnoser Agent
    participant E as Executive Agent

    %% Initial Alert
    H->>G: "Auth0 unauthorized exception on prod"
    Note over G: Generate event_id<br>Capture thread_ts<br>Add ISO timestamp<br>Check dedup cache
    G->>K: Publish to `slack-inbound`
    
    %% Parallel Processing
    par Asynchronous Event Consumption
        K-->>S: Consume `slack-inbound`
        K-->>D: Consume `slack-inbound` (buffered)
        K-->>E: Consume `slack-inbound` (ignored)
    end
    
    %% Scout Action
    Note over S: Extract keywords:<br>"Auth0 unauthorized exception"
    S->>S: Search Exa with extracted query
    S->>K: Publish findings to `agent-context`<br>(with event_id + thread_ts)
    
    %% Diagnoser Action
    K-->>D: Consume `agent-context`
    Note over D: Build prompt with:<br>• Original error<br>• Exa research<br>• Buffered conversation history
    D->>D: Call OpenRouter for severity + diagnosis
    D->>K: Publish to `slack-outbound`<br>(with thread_ts for in-thread reply)
    
    %% Gateway Threads Reply
    K-->>G: Consume `slack-outbound`
    G->>H: Reply IN THREAD: "Sev-1 Diagnosis: ..."
    
    %% Executive Action
    H->>G: "execute rollback" (in same thread)
    G->>K: Publish to `slack-inbound`
    
    K-->>E: Consume `slack-inbound`
    Note over E: Command detected
    E->>E: Check cached Auth0 token<br>(skip fetch if still valid)
    E->>E: Execute Rollback Tool
    E->>K: Publish to `slack-outbound`
    
    K-->>G: Consume `slack-outbound`
    G->>H: Reply IN THREAD: "Rollback successful ✅"
```

## 3. ToolGrad Data Generation Flow

The **Executive Agent** possesses dangerous capabilities. To ensure reliability, we implement the **ToolGrad** methodology with a real stateful sandbox, textual gradient optimization loop, and inverse query synthesis.

```mermaid
flowchart TD
    Start([Start ToolGrad Engine]) --> Schema[Extract Tool Schemas<br>auth0_verify, rollback, restart]
    Schema --> Sandbox[Initialize ProductionSandbox<br>with realistic state]
    Sandbox --> Plan[Generate intentionally imperfect plan<br>wrong scope, wrong env, missing auth]
    
    Plan --> Loop{Execute Step in Sandbox}
    Loop -->|Success| NextStep{More steps?}
    NextStep -->|Yes| Loop
    NextStep -->|No| Verified[Verified Tool Chain ✅]
    
    Loop -->|Failure| Critic[[LLM Critic via OpenRouter<br>Generate Textual Gradient]]
    Critic --> Apply[Apply gradient to fix arguments]
    Apply --> Retry{Retries left?}
    Retry -->|Yes| Loop
    Retry -->|No| Fail[Mark scenario as failed]
    
    Verified --> Inverse[[Inverse Query Synthesis<br>Generate realistic Slack message]]
    Inverse --> Pair[Pair query + verified chain]
    Pair --> Save[Save to toolgrad_dataset.jsonl<br>OpenAI fine-tuning format]
    Save --> G([Ready for Mozilla.ai Fine-Tuning])

    style Critic stroke:#f66,stroke-width:2px,stroke-dasharray: 5 5
    style G fill:#9f9,stroke:#333,stroke-width:2px
```

## 4. Production Reliability Features

```mermaid
graph LR
    subgraph "Reliability Patterns"
        A[Correlation IDs] --> B[Event Traceability]
        C[Dead Letter Queue] --> D[No Silent Data Loss]
        E[Graceful Shutdown] --> F[Clean Consumer Groups]
        G[Message Dedup] --> H[No Double Processing]
        I[Auth0 Token Cache] --> J[Reduced API Calls]
        K[Slack Rate Limiting] --> L[No 429 Errors]
        M[In-Thread Replies] --> N[Clean War Room Channel]
    end
```
