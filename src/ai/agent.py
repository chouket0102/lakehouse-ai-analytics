"""
Air Quality Agent using LangChain (v0.2+ Compatible).
"""
import os
from typing import Any, Dict

# --- Standard Imports ---
# If these fail, the environment is broken and agent_serving.py should have caught it.
try:
    from langchain_openai import ChatOpenAI
    from langchain_core.tools import Tool
    from langchain_core.prompts import ChatPromptTemplate
    # The standard path for AgentExecutor in modern LangChain
    from langchain.agents import AgentExecutor, create_tool_calling_agent
except ImportError:
    # Fallback for very specific messed up environments (rare)
    from langchain.agents.agent import AgentExecutor
    from langchain.agents import create_tool_calling_agent
    from langchain_community.chat_models import ChatOpenAI

# Local tools
from src.ai.tools import get_current_air_quality, compare_city_risk

def create_air_quality_agent(model_name: str = "gpt-4") -> AgentExecutor:
    """
    Initializes a robust, tool-calling LangChain agent.
    """
    # 1. Initialize LLM
    llm = ChatOpenAI(
        model_name=model_name,
        temperature=0.0
    )

    # 2. Define Tools
    tools = [
        Tool(
            name="GetAirQualityStatus",
            func=get_current_air_quality,
            description="Use this tool to get the latest pollution values and health risk for a single city. Input should be a city name."
        ),
        Tool(
            name="CompareCities",
            func=compare_city_risk,
            description="Use this tool to compare the air quality health risk between exactly two specified cities. Input should be two city names."
        )
    ]

    # 3. Define Prompt
    system_message = """You are a professional Air Quality Analyst AI. Your goal is to answer user questions about pollution 
and health risk by judiciously using the provided tools. Be concise, accurate, and helpful. 
Use the tools if a specific city or comparison is requested."""

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_message),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ])
    
    # 4. Construct Agent
    agent = create_tool_calling_agent(llm, tools, prompt)
    
    agent_executor = AgentExecutor(
        agent=agent, 
        tools=tools, 
        verbose=True, 
        handle_parsing_errors=True
    )

    return agent_executor