try:
    import fastapi
    import uvicorn
    import langchain
    import langchain_openai
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "fastapi", "uvicorn", "langchain", "langchain-openai"])

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime
import uvicorn
import os

from src.ai.agent import create_air_quality_agent

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

class HealthRecommendationRequest(BaseModel):
    location_name: str
    activity: str = "general"
    sensitive_group: bool = False

class CompareLocationsRequest(BaseModel):
    locations: List[str]
    period_hours: int = 24

class MonitoringRequest(BaseModel):
    locations: List[str]
    alert_threshold_aqi: int = 150

class DailyReportRequest(BaseModel):
    locations: List[str]


agent = None

@app.on_event("startup")
def startup_event():
    global agent
    try:
        # Initialize agent
        agent = create_air_quality_agent()
        logger.info("Agent initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize agent: {str(e)}")
        pass


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "agent_initialized": agent is not None,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/v1/chat")
async def chat(request: ChatRequest):
    if not agent:
        raise HTTPException(status_code=503, detail="Agent not initialized")
    
    try:
        logger.info(f"Chat request: {request.message}")
        response = agent.run(request.message)
        
        return {
            "response": response,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/current_conditions")
async def get_current_conditions(location_name: Optional[str] = None, location_id: Optional[int] = None):

    from src.ai.tools import get_current_air_quality
    
    try:
        if not location_name and not location_id:
            raise HTTPException(status_code=400, detail="Must provide location_name or location_id")
            
        result = get_current_air_quality(location_name=location_name, location_id=location_id)
        
        if "error" in result:
             raise HTTPException(status_code=404, detail=result["error"])
             
        return {
            "data": result,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
         logger.error(f"Error: {e}")
         raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/compare_locations")
async def compare_locations(request: CompareLocationsRequest):
    from src.ai.tools import compare_city_risk
    from src.ai.tools import _tools_instance, AirQualityTools
    
    tools_instance = _tools_instance or AirQualityTools()
    
    try:
        result = tools_instance.compare_locations(request.locations, request.period_hours)
        if "error" in result:
             raise HTTPException(status_code=404, detail=result["error"])
        
        return {
            "data": result,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--openai_api_key", help="OpenAI API Key")
    args, unknown = parser.parse_known_args()
    
    if args.openai_api_key:
        os.environ["OPENAI_API_KEY"] = args.openai_api_key
        
    uvicorn.run("src.ai.agent_serving:app", host="0.0.0.0", port=8000, reload=False)
