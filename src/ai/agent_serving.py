"""
AirQuality Agent Deployment and API Serving
FastAPI app for serving the agent + Databricks Job scheduling hooks
"""

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

# --- Global Agent State ---
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
        # We don't raise here to allow the app to start, but health check will fail
        pass

# --- API Endpoints ---

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
    # This endpoint bypasses the agent for direct tool access if needed, 
    # OR we can just use the agent. Let's use the tool logic if accessible, 
    # but since tools are wrapped in the agent, we might want to just ask the agent 
    # or expose tools directly. 
    # For now, we'll keep the FastAPI structure simple and rely on the agent for 'chat',
    # but if we need direct data, we should use the tools directly.
    
    # Import here to avoid circular dependency if tools imports this file (unlikely)
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
    # Note: compare_city_risk in tools.py expects 2 cities as args (city1, city2) 
    # but the API request has a list.
    # The tool refactor for `compare_city_risk` wrapper supports 2.
    # If the user sends more, `compare_locations` in `AirQualityTools` supports list.
    
    # We should access the underlying tools instance directly for full flexibility
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
    uvicorn.run("src.ai.agent_serving:app", host="0.0.0.0", port=8000, reload=True)
