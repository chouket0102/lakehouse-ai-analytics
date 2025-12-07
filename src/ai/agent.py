from langchain.agents import initialize_agent, AgentType, Tool
from langchain_community.chat_models import ChatDatabricks
from src.ai.tools import get_current_air_quality, compare_city_risk

def create_air_quality_agent(model_endpoint: str = "databricks-dbrx-instruct"):
    """
    Initializes a robust, tool-calling LangChain agent tailored for air quality analysis.
    
    Args:
        model_endpoint: The Databricks Model Serving Endpoint to use.
    """
    llm = ChatDatabricks(
        endpoint=model_endpoint, 
        temperature=0.0

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
    
    # 4. Agent System Prompt (makes the agent more professional)
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
    # NOTE: This only works if run inside a Databricks Notebook environment with the LLM running.
    # Replace 'databricks-dbrx-instruct' with your actual endpoint name if needed.
    agent = create_air_quality_agent()
    
    # Test 1: Tool Call
    print("--- Test 1: Single City Status ---")
    agent.run("What is the air quality risk in Trapani?")
    
    # Test 2: Comparison Tool Call
    print("\n--- Test 2: City Comparison ---")
    agent.run("Compare the pollution risk between Trapani and Rome.")