from langchain.agents import initialize_agent, AgentType, Tool
from langchain_openai import ChatOpenAI
from src.ai.tools import get_current_air_quality, compare_city_risk
import os

def create_air_quality_agent(model_name: str = "gpt-4"):
    """
    Initializes a robust, tool-calling LangChain agent tailored for air quality analysis.
    
    Args:
        model_name: The OpenAI model name to use.
    """
    llm = ChatOpenAI(
        model_name=model_name,
        temperature=0.0
    )

    tools = [
        Tool(
            name="GetAirQualityStatus",
            func=get_current_air_quality,
            description="Use this tool to get the latest pollution values and health risk for a single city."
        ),
        Tool(
            name="CompareCities",
            func=compare_city_risk,
            description="Use this tool to compare the air quality health risk between exactly two specified cities."
        )
    ]

    agent = initialize_agent(
        tools=tools,
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        handle_parsing_errors=True, 
        max_iterations=5 
    )
    
    # Agent System Prompt
    agent.agent.llm_chain.prompt.template = """
    You are a professional Air Quality Analyst AI. Your goal is to answer user questions about pollution 
    and health risk by judiciously using the provided tools. Be concise, accurate, and helpful. 
    Use the tools if a specific city or comparison is requested.
    
    TOOLS:
    {tools}
    
    USER INPUT: {input}
    
    RESPONSE:
    """

    return agent

# Example usage 
if __name__ == "__main__":
    # Ensure OPENAI_API_KEY is set in the environment
    agent = create_air_quality_agent()
    
    # Test 1: Tool Call
    print("--- Test 1: Single City Status ---")
    agent.run("What is the air quality risk in Trapani?")
    
    # Test 2: Comparison Tool Call
    print("\n--- Test 2: City Comparison ---")
    agent.run("Compare the pollution risk between Trapani and Rome.")