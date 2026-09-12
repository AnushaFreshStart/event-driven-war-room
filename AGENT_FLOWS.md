# Agent Workflows & Event Contracts

In an event-driven architecture, the logic of individual microservices (agents) and the shape of the data they pass (the event contracts) are the most critical pieces of documentation.

Below are the detailed internal flow diagrams for each of the three AI agents, followed by the Kafka Topic Event Contracts.

---

## 1. Scout Agent (The Researcher)

The Scout Agent acts as an ambient listener. It uses keyword extraction to isolate error signatures from conversational noise before querying Exa, reducing unnecessary API calls and improving search quality.

```mermaid
flowchart TD
    Start([Start Scout Agent]) --> Consume[Consume message from 'slack-inbound' topic]
    Consume --> ParseJSON{Parse JSON}
    
    ParseJSON -->|Parse Error| DLQ[Publish raw bytes to 'dead-letter' topic]
    DLQ --> Consume
    
    ParseJSON -->|Success| Check{Contains error keywords? <br> error, exception, traceback, failed, <br> timeout, crash, outage, 500-504, OOM, panic}
    
    Check -->|No| Ignore[Ignore Event / Await Next]
    Ignore --> Consume
    
    Check -->|Yes| Extract[Extract search query from text <br> Strip conversational filler <br> Isolate error codes & stack traces]
    Extract --> CallExa[[Call Exa Search API]]
    CallExa --> Format[Format findings into 'research_context']
    Format --> Package[Construct 'context_event' JSON <br> Include event_id, thread_ts, timestamp]
    Package --> Publish[Publish to 'agent-context' topic]
    Publish --> Consume
```

---

## 2. Diagnoser Agent (The Brain)

The Diagnoser subscribes to **both** `slack-inbound` and `agent-context`. It maintains a sliding window of recent channel messages so the LLM has full conversation history when synthesizing a diagnosis, not just a single error line.

```mermaid
flowchart TD
    Start([Start Diagnoser Agent]) --> Consume[Consume message from <br> 'slack-inbound' OR 'agent-context']
    Consume --> ParseJSON{Parse JSON}
    
    ParseJSON -->|Parse Error| DLQ[Publish raw bytes to 'dead-letter' topic]
    DLQ --> Consume
    
    ParseJSON -->|Success| WhichTopic{Which topic?}
    
    WhichTopic -->|slack-inbound| Buffer[Store in per-channel <br> sliding window buffer <br> max 20 messages]
    Buffer --> Consume
    
    WhichTopic -->|agent-context| Gather[Retrieve buffered conversation <br> history for this channel]
    Gather --> Prompt[Construct enriched system prompt: <br> • Severity assessment Sev-1 to Sev-4 <br> • Affected service identification <br> • Immediate mitigation steps <br> • Rollback recommendation]
    Prompt --> CallLLM[[Call OpenRouter API]]
    CallLLM --> Format[Format Diagnosis with severity tag]
    Format --> Package[Construct 'response_event' JSON <br> Include event_id, thread_ts, timestamp]
    Package --> Publish[Publish to 'slack-outbound' topic]
    Publish --> Consume
```

---

## 3. Executive Agent (The Actor)

The Executive Agent listens for explicit action commands. It enforces a strict Auth0 authorization gate with **token caching** (tokens are cached for 24 hours with a 5-minute safety buffer) and supports multiple commands.

```mermaid
flowchart TD
    Start([Start Executive Agent]) --> Consume[Consume message from 'slack-inbound' topic]
    Consume --> ParseJSON{Parse JSON}
    
    ParseJSON -->|Parse Error| DLQ[Publish raw bytes to 'dead-letter' topic]
    DLQ --> Consume
    
    ParseJSON -->|Success| Check{Contains command?}
    
    Check -->|No match| Ignore[Ignore Event / Await Next]
    Ignore --> Consume
    
    Check -->|execute rollback<br>restart service<br>scale up| Extract[Extract 'user_id' and command type]
    Extract --> TokenCache{Cached Auth0 <br> token valid?}
    
    TokenCache -->|Yes| UseToken[Use cached token]
    TokenCache -->|No / Expired| Auth0[[Fetch new token from Auth0 <br> Cache with 5-min buffer]]
    Auth0 --> UseToken
    
    UseToken --> Verify{User authorized?}
    
    Verify -->|No| Reject[Format Rejection Message]
    Reject --> Package
    
    Verify -->|Yes| Tool[[Execute Action <br> Rollback / Restart / Scale]]
    Tool --> Success[Format Success Message]
    Success --> Package[Construct 'response_event' JSON <br> Include event_id, thread_ts, timestamp]
    
    Package --> Publish[Publish to 'slack-outbound' topic]
    Publish --> Consume
```

---

## 4. Slack Gateway (The Bridge)

The Gateway bridges Slack and Kafka with production-grade reliability features.

```mermaid
flowchart TD
    subgraph "Inbound (Slack → Kafka)"
        SlackIn[Slack SocketMode Event] --> Dedup{Seen this <br> client_msg_id?}
        Dedup -->|Duplicate| Drop[Drop silently]
        Dedup -->|New| Enrich[Generate UUID event_id <br> Capture thread_ts <br> Add ISO timestamp]
        Enrich --> PubIn[Publish to 'slack-inbound']
    end

    subgraph "Outbound (Kafka → Slack)"
        ConOut[Consume from 'slack-outbound'] --> ParseOut{Parse JSON}
        ParseOut -->|Error| DLQ[Publish to 'dead-letter']
        ParseOut -->|Success| Post[chat_postMessage <br> with thread_ts]
        Post --> RateCheck{HTTP 429?}
        RateCheck -->|No| Done[Delivered]
        RateCheck -->|Yes| Backoff[Sleep Retry-After <br> Retry up to 3x]
        Backoff --> Post
    end
```

---

## 5. Event Stream Contracts (Data Dictionary)

### Topic: `slack-inbound`
**Produced by:** Slack Gateway
**Consumed by:** Scout Agent, Diagnoser Agent, Executive Agent
```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "channel_id": "C12345678",
  "user_id": "U98765432",
  "text": "We just got an exception: Auth0 unauthorized audience error on the prod server.",
  "thread_ts": "1726182000.000100",
  "timestamp": "2026-09-12T17:18:10Z",
  "is_mention": false
}
```

### Topic: `agent-context`
**Produced by:** Scout Agent
**Consumed by:** Diagnoser Agent
```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "channel_id": "C12345678",
  "thread_ts": "1726182000.000100",
  "timestamp": "2026-09-12T17:18:12Z",
  "original_text": "Auth0 unauthorized audience error on the prod server",
  "research_context": "Exa Search Results: ...",
  "source": "exa_scout"
}
```

### Topic: `slack-outbound`
**Produced by:** Diagnoser Agent, Executive Agent
**Consumed by:** Slack Gateway
```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "channel_id": "C12345678",
  "thread_ts": "1726182000.000100",
  "text": ":mag: *AI Diagnosis* (Sev-1)\n..."
}
```

### Topic: `dead-letter`
**Produced by:** Any agent on JSON parse failure
**Consumed by:** Ops/Debug tooling
```
Raw bytes of the unparseable message for inspection.
```
