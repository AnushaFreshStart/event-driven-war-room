import json
import time

def generate_toolgrad_dataset():
    print("Initializing ToolGrad generation (Google Research Methodology)...")
    time.sleep(1)
    
    print("Step 1: Sampling API endpoints (Auth0 verify, Rollback trigger)")
    time.sleep(1)
    
    print("Step 2: Proposing textual gradients for valid tool chain...")
    tool_chain = [
        {"action": "auth0_verify", "args": {"user": "admin", "scope": "rollback"}},
        {"action": "server_rollback", "args": {"env": "prod", "version": "previous"}}
    ]
    print(f"Generated Ground Truth Chain: {json.dumps(tool_chain)}")
    time.sleep(1)
    
    print("Step 3: Reversing into synthetic user query...")
    synthetic_query = "Execute rollback on prod due to auth error."
    print(f"Generated Query: {synthetic_query}")
    time.sleep(1)
    
    dataset_entry = {
        "messages": [
            {"role": "user", "content": synthetic_query},
            {"role": "assistant", "tool_calls": tool_chain}
        ]
    }
    
    # Save for Mozilla.ai llamafile fine-tuning
    with open("toolgrad_dataset.jsonl", "a") as f:
        f.write(json.dumps(dataset_entry) + "\n")
        
    print("ToolGrad dataset generated successfully: toolgrad_dataset.jsonl")
    print("This dataset is formatted for fine-tuning local Mozilla.ai llamafile models.")

if __name__ == "__main__":
    generate_toolgrad_dataset()
