"""
Air Quality Agent using LangChain with version compatibility.
"""
from typing import Any, Dict
import os

# Version-compatible imports
try:
    from langchain.agents import Tool, AgentExecutor
    from langchain_openai import ChatOpenAI
except ImportError:
    from langchain.agents import Tool, AgentExecutor
    from langchain.chat_models import ChatOpenAI

from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.agents.conversational_chat.base import ConversationalChatAgent

from src.ai.tools import get_current_air_quality, compare_city_risk


def create_air_quality_agent(model_name: str = "gpt-4"):
    """
    Initializes a robust, tool-calling LangChain agent tailored for air quality analysis.
    
    Args:
        model_name: The OpenAI model name to use.
    
    Returns:
        AgentExecutor: The configured agent executor ready to run queries.
    """
    llm = ChatOpenAI(
        model_name=model_name,
        temperature=0.0
    )

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

    # System message for the agent
    system_message = """You are a professional Air Quality Analyst AI. Your goal is to answer user questions about pollution 
and health risk by judiciously using the provided tools. Be concise, accurate, and helpful. 
Use the tools if a specific city or comparison is requested."""

    # Create the agent using ConversationalChatAgent which is more stable across versions
    agent = ConversationalChatAgent.from_llm_and_tools(
        llm=llm,
        tools=tools,
        system_message=system_message,
        verbose=True
    )
    
    # Create the AgentExecutor
    agent_executor = AgentExecutor.from_agent_and_tools(
        agent=agent,
        tools=tools,
        verbose=True,
        handle_parsing_errors=True,
        max_iterations=5
    )

    return agent_executor

# Example usage 
if __name__ == "__main__":
    # Ensure OPENAI_API_KEY is set in the environment
    agent_executor = create_air_quality_agent()
    
    # Test 1: Tool Call
    print("--- Test 1: Single City Status ---")
    result = agent_executor.run("What is the air quality risk in Trapani?")
    print(result)
    
    # Test 2: Comparison Tool Call
    print("\n--- Test 2: City Comparison ---")
    result = agent_executor.run("Compare the pollution risk between Trapani and Rome.")
    print(result)