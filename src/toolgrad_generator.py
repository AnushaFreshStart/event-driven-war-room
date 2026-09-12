import os
import json
import copy
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv
from openai import OpenAI

# Load .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# 1. Define Tool Schemas
TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "auth0_verify_permission",
            "description": "Verify user permissions",
            "parameters": {
                "type": "object",
                "properties": {
                    "user_id": {"type": "string"},
                    "required_scope": {"type": "string"}
                },
                "required": ["user_id", "required_scope"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "rollback_deployment",
            "description": "Rollback a deployment",
            "parameters": {
                "type": "object",
                "properties": {
                    "environment": {"type": "string"},
                    "target_version": {"type": "string"},
                    "is_authorized": {"type": "boolean"}
                },
                "required": ["environment", "target_version", "is_authorized"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "restart_service",
            "description": "Restart a specific service",
            "parameters": {
                "type": "object",
                "properties": {
                    "service_name": {"type": "string"},
                    "is_authorized": {"type": "boolean"}
                },
                "required": ["service_name", "is_authorized"]
            }
        }
    }
]

# 2. ProductionSandbox Class
class ProductionSandbox:
    def __init__(self):
        self.initial_state = {
            "deployments": {
                "prod": {"version": "v1.2.0", "status": "failing"},
                "staging": {"version": "v1.3.0", "status": "healthy"}
            },
            "services": {
                "payment_gateway": {"status": "degraded"},
                "auth_service": {"status": "healthy"}
            },
            "users": {
                "admin_alice": {"roles": ["admin"], "scopes": ["rollback", "restart"]},
                "dev_bob": {"roles": ["developer"], "scopes": ["restart"]},
                "intern_charlie": {"roles": ["viewer"], "scopes": []}
            }
        }
        self.state = copy.deepcopy(self.initial_state)

    def reset(self):
        self.state = copy.deepcopy(self.initial_state)

    def get_snapshot(self) -> dict:
        return copy.deepcopy(self.state)

    def restore_snapshot(self, snapshot: dict):
        self.state = copy.deepcopy(snapshot)

    def execute_tool(self, tool_name: str, arguments: dict) -> Tuple[bool, str, dict]:
        if tool_name == "auth0_verify_permission":
            user_id = arguments.get("user_id")
            req_scope = arguments.get("required_scope")
            if user_id not in self.state["users"]:
                return False, f"User {user_id} not found", {"error_type": "user_not_found"}
            if req_scope not in self.state["users"][user_id]["scopes"]:
                return False, f"User {user_id} lacks scope {req_scope}", {"error_type": "insufficient_scope"}
            return True, "Permission verified successfully.", {"authorized_scope": req_scope}

        elif tool_name == "rollback_deployment":
            env = arguments.get("environment")
            version = arguments.get("target_version")
            is_authorized = arguments.get("is_authorized", False)
            if not is_authorized:
                return False, "Not authorized. Must verify permissions first.", {"error_type": "unauthorized"}
            if env not in self.state["deployments"]:
                return False, f"Invalid environment {env}", {"error_type": "invalid_environment"}
            if not isinstance(version, str) or not version.startswith("v"):
                return False, "Target version must start with 'v'", {"error_type": "invalid_version"}
            
            self.state["deployments"][env]["version"] = version
            self.state["deployments"][env]["status"] = "healthy"
            return True, f"Successfully rolled back {env} to {version}.", {"new_version": version}

        elif tool_name == "restart_service":
            svc = arguments.get("service_name")
            is_authorized = arguments.get("is_authorized", False)
            if not is_authorized:
                return False, "Not authorized. Must verify permissions first.", {"error_type": "unauthorized"}
            if svc not in self.state["services"]:
                return False, f"Service {svc} not found", {"error_type": "invalid_service"}
            
            self.state["services"][svc]["status"] = "healthy"
            return True, f"Successfully restarted {svc}.", {"new_status": "healthy"}

        return False, f"Unknown tool: {tool_name}", {"error_type": "unknown_tool"}


def get_llm_client():
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        return None
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key
    )

def local_heuristic_fix(tool_name: str, arguments: dict, meta: dict) -> dict:
    args = copy.deepcopy(arguments)
    if tool_name == "auth0_verify_permission":
        if meta.get("error_type") == "insufficient_scope":
            args["required_scope"] = "rollback" if "rollback" not in args.get("required_scope", "") else "restart"
    elif tool_name == "rollback_deployment":
        if meta.get("error_type") == "invalid_environment":
            args["environment"] = "prod"
        elif meta.get("error_type") == "invalid_version":
            args["target_version"] = "v1.1.0"
        elif meta.get("error_type") == "unauthorized":
            args["is_authorized"] = True
    elif tool_name == "restart_service":
        if meta.get("error_type") == "invalid_service":
            args["service_name"] = "payment_gateway"
        elif meta.get("error_type") == "unauthorized":
            args["is_authorized"] = True
    return args

# 3. ToolGrad Optimization Loop
def optimize_tool_plan(sandbox: ProductionSandbox, initial_plan: List[Dict[str, Any]]) -> Tuple[bool, List[Dict[str, Any]], List[Tuple[Dict, Dict, Dict]]]:
    client = get_llm_client()
    optimized_plan = []
    state_transitions = []
    
    sandbox.reset()
    current_plan = copy.deepcopy(initial_plan)
    
    overall_success = True
    
    for step_idx, step in enumerate(current_plan):
        tool_name = step["name"]
        arguments = step["arguments"]
        
        success = False
        retries = 3
        while not success and retries > 0:
            pre_state = sandbox.get_snapshot()
            success, output, meta = sandbox.execute_tool(tool_name, arguments)
            
            if success:
                optimized_plan.append({"name": tool_name, "arguments": copy.deepcopy(arguments)})
                state_transitions.append((pre_state, sandbox.get_snapshot(), {"name": tool_name, "output": output}))
                break
            
            # Textual gradient from LLM or fallback
            retries -= 1
            if client:
                try:
                    prompt = f"Tool '{tool_name}' failed with args: {json.dumps(arguments)}. Error: {output}. Return only the corrected JSON object for the arguments."
                    response = client.chat.completions.create(
                        model="openai/gpt-4o-mini",
                        messages=[{"role": "user", "content": prompt}]
                    )
                    gradient = response.choices[0].message.content.strip()
                    if "```json" in gradient:
                        gradient = gradient.split("```json")[1].split("```")[0].strip()
                    elif "```" in gradient:
                        gradient = gradient.split("```")[1].split("```")[0].strip()
                    arguments = json.loads(gradient)
                except Exception:
                    arguments = local_heuristic_fix(tool_name, arguments, meta)
            else:
                arguments = local_heuristic_fix(tool_name, arguments, meta)
                
        if not success:
            overall_success = False
            break

    return overall_success, optimized_plan, state_transitions

# 4. Inverse Query Synthesis
def synthesize_query(optimized_plan: List[Dict], state_transitions: List) -> str:
    client = get_llm_client()
    if client:
        try:
            prompt = f"Given this executed tool plan: {json.dumps(optimized_plan)}, write a single, realistic Slack incident message from a user asking an AI assistant to perform these actions. Do not wrap in quotes."
            response = client.chat.completions.create(
                model="openai/gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}]
            )
            return response.choices[0].message.content.strip(' "')
        except:
            pass
    
    # Fallback
    tools_used = [t['name'] for t in optimized_plan]
    if "rollback_deployment" in tools_used:
        return "Critical issue in prod, please verify my auth and rollback the deployment."
    elif "restart_service" in tools_used:
        return "Service is degraded, verify my auth and restart the service."
    return "Execute the required actions."


def generate_dataset():
    sandbox = ProductionSandbox()
    dataset = []

    # 5. Generate multiple dataset entries
    print("Generating scenario 1...")
    plan1 = [
        {"name": "auth0_verify_permission", "arguments": {"user_id": "admin_alice", "required_scope": "read"}},
        {"name": "rollback_deployment", "arguments": {"environment": "production", "target_version": "1.1.0", "is_authorized": False}}
    ]
    succ1, opt_plan1, transitions1 = optimize_tool_plan(sandbox, plan1)
    if succ1:
        query1 = synthesize_query(opt_plan1, transitions1)
        dataset.append({
            "messages": [
                {"role": "user", "content": query1},
                {"role": "assistant", "tool_calls": [{"id": f"call_{i}", "type": "function", "function": {"name": p["name"], "arguments": json.dumps(p["arguments"])}} for i, p in enumerate(opt_plan1)]}
            ]
        })

    print("Generating scenario 2...")
    plan2 = [
        {"name": "auth0_verify_permission", "arguments": {"user_id": "dev_bob", "required_scope": "restart"}},
        {"name": "restart_service", "arguments": {"service_name": "payments", "is_authorized": False}}
    ]
    succ2, opt_plan2, transitions2 = optimize_tool_plan(sandbox, plan2)
    if succ2:
        query2 = synthesize_query(opt_plan2, transitions2)
        dataset.append({
            "messages": [
                {"role": "user", "content": query2},
                {"role": "assistant", "tool_calls": [{"id": f"call_{i}", "type": "function", "function": {"name": p["name"], "arguments": json.dumps(p["arguments"])}} for i, p in enumerate(opt_plan2)]}
            ]
        })

    print("Generating scenario 3...")
    plan3 = [
        {"name": "auth0_verify_permission", "arguments": {"user_id": "intern_charlie", "required_scope": "rollback"}},
        {"name": "rollback_deployment", "arguments": {"environment": "prod", "target_version": "v1.0.0", "is_authorized": False}}
    ]
    succ3, opt_plan3, transitions3 = optimize_tool_plan(sandbox, plan3)
    if not succ3:
        query3 = "I'm the intern, please rollback prod immediately!"
        dataset.append({
            "messages": [
                {"role": "user", "content": query3},
                {"role": "assistant", "tool_calls": [{"id": "call_0", "type": "function", "function": {"name": "auth0_verify_permission", "arguments": json.dumps({"user_id": "intern_charlie", "required_scope": "rollback"})}}]}
            ]
        })
        
    dataset_path = os.path.join(os.path.dirname(__file__), '..', 'toolgrad_dataset.jsonl')
    print(f"Writing to {dataset_path}...")
    with open(dataset_path, "w") as f:
        for entry in dataset:
            f.write(json.dumps(entry) + "\n")
            
    print(f"Generated {len(dataset)} entries in {dataset_path}")

if __name__ == "__main__":
    generate_dataset()
