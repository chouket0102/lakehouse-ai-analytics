from langchain.agents import create_react_agent, AgentExecutor, Tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from src.ai.tools import get_current_air_quality, compare_city_risk
import os

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

    # Create a custom prompt template for the ReAct agent
    template = """You are a professional Air Quality Analyst AI. Your goal is to answer user questions about pollution 
and health risk by judiciously using the provided tools. Be concise, accurate, and helpful. 
Use the tools if a specific city or comparison is requested.

You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {input}
Thought:{agent_scratchpad}"""

    prompt = PromptTemplate.from_template(template)
    
    # Create the ReAct agent
    agent = create_react_agent(llm, tools, prompt)
    
    # Create the AgentExecutor
    agent_executor = AgentExecutor(
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
    result = agent_executor.invoke({"input": "What is the air quality risk in Trapani?"})
    print(result)
    
    # Test 2: Comparison Tool Call
    print("\n--- Test 2: City Comparison ---")
    result = agent_executor.invoke({"input": "Compare the pollution risk between Trapani and Rome."})
    print(result)