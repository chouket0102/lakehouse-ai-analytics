import sys
import subprocess
import os
import logging
import importlib
from datetime import datetime
from typing import List, Optional, Dict, Any

# --- 1. SELF-HEALING & DEPENDENCY CHECK ---
def ensure_environment_ready():
    """
    Checks for required packages and installs them if missing.
    CRITICAL: Pins numpy<2 to avoid breaking Databricks Runtime.
    """
    required_modules = [
        "fastapi", "uvicorn", "langchain", "langchain_community", 
        "langchain_openai", "langchain_core"
    ]
    
    missing = []
    for module in required_modules:
        try:
            importlib.import_module(module)
        except ImportError:
            missing.append(module)
    
    # Also check specifically for AgentExecutor availability
    try:
        from langchain.agents import AgentExecutor
    except ImportError:
        if "langchain" not in missing:
            missing.append("langchain (update needed)")

    if missing:
        print(f">>> Missing or broken dependencies detected: {missing}")
        print(">>> STARTING AUTO-REPAIR (This may take a minute)...")
        try:
            # We install numpy<2 because Databricks usually relies on numpy 1.x
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", 
                "--upgrade",
                "numpy<2",  # CRITICAL FIX for Databricks
                "fastapi", 
                "uvicorn", 
                "langchain", 
                "langchain-community", 
                "langchain-core", 
                "langchain-openai",
                "pydantic"
            ])
            print("\n" + "="*60)
            print("  ENVIRONMENT UPDATED SUCCESSFULLY")
            print("  PLEASE RESTART YOUR KERNEL/COMPUTE NOW.")
            print("  (The script cannot continue until the new files are loaded)")
            print("="*60 + "\n")
            # We exit here because continuing with mixed memory states causes the ImportError you saw.
            sys.exit(0) 
        except subprocess.CalledProcessError as e:
            print(f">>> CRITICAL FAILURE during install: {e}")
            raise

# Run the check
ensure_environment_ready()

# --- 2. APP IMPORTS ---
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

# We import the agent AFTER the environment check
try:
    from src.ai.agent import create_air_quality_agent
except ImportError as e:
    # If this fails immediately after a restart, it's a code issue
    logging.error(f"Failed to import agent code: {e}")
    raise

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Air Quality Agent API", version="1.0.0")

# --- Pydantic Models ---
class ChatRequest(BaseModel):
    message: str
    conversation_history: List[Dict[str, str]] = []

class CompareLocationsRequest(BaseModel):
    locations: List[str]
    period_hours: int = 24

# --- Global State ---
agent_executor = None

@app.on_event("startup")
def startup_event():
    global agent_executor
    try:
        agent_executor = create_air_quality_agent()
        logger.info("Agent initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize agent: {str(e)}")

@app.get("/health")
def health_check():
    return {
        "status": "healthy" if agent_executor else "degraded",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/v1/chat")
async def chat(request: ChatRequest):
    if not agent_executor:
        raise HTTPException(status_code=503, detail="Agent system not initialized. Check server logs.")
    
    try:
        # invoke() is the modern API
        response_obj = agent_executor.invoke({"input": request.message})
        output = response_obj.get("output", str(response_obj))
        return {"response": output, "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Chat error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/current_conditions")
async def get_current_conditions(location_name: Optional[str] = None):
    # Lazy import to prevent circular dependency issues
    from src.ai.tools import get_current_air_quality
    try:
        if not location_name:
            raise HTTPException(status_code=400, detail="location_name is required")
        result = get_current_air_quality(location_name=location_name)
        if isinstance(result, dict) and "error" in result:
             raise HTTPException(status_code=404, detail=result["error"])
        return {"data": result, "status": "success"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/compare_locations")
async def compare_locations(request: CompareLocationsRequest):
    from src.ai.tools import compare_city_risk
    try:
        if len(request.locations) != 2:
            raise HTTPException(status_code=400, detail="Must provide exactly 2 locations")
        result = compare_city_risk(request.locations[0], request.locations[1])
        return {"data": result, "status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--openai_api_key", help="OpenAI API Key")
    args, unknown = parser.parse_known_args()
    
    if args.openai_api_key:
        os.environ["OPENAI_API_KEY"] = args.openai_api_key
        
    uvicorn.run("src.ai.agent_serving:app", host="0.0.0.0", port=8000, reload=False)