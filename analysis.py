import asyncio
import sqlite3
import pandas as pd
import autogen

query_db_declaration = {
    "name": "query_db",
    "description": "Query function to query the sqlite db.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "A detailed sql query to return the necessary information from the sqlite db.",
            },
        },
        "required": ["query"],
    },
}

def query_db(query, db_path="stock_data.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

config_list = autogen.filter_config(
    config_list=[
        {
            "model": "gpt-4o",
            "api_key": "",
        }
    ],
    filter_dict=None,
)


# Define a termination message checker
def is_termination_msg(data):
    has_content = "content" in data and data["content"] is not None
    return has_content and "TERMINATE" in data["content"]


# Define the chat initiation function
async def stock_research():

    manager_agent = autogen.AssistantAgent(
        name="manager_agent",
        system_message="""
        You are a manager. You are harsh but analytical. You are relentless but fair.

        Follow this process in the exact order specified, ensuring each step is completed before moving to the next:
        1. Ask the 'hypothesis_agent' to come up with a hypothesis for a possible profitable stock trading strategy.
        2. After the 'hypothesis_agent' is done, ask 'query_agent' to collect the necessary data from the sqlite database to test the theory. Limit all pricing queries to no more than 1000 rows. Use a query to calculate the ROI and report it.
        3. After the 'query_agent' is done, ask 'quality_agent' to determine the ROI on the strategy given the data returned from 'query_agent'.
        4. If the reported ROI is less than 5% annually, report the findings to the 'hypothesis_agent' and instruct it to either tweak the hypothesis or develop a new one. Do not stop iterating until we hit the desired ROI. Then repeat steps 2-4.
        5. After step 5, you must ask the 'report_generator_agent' to generate a final report that states the ROI and lists every trade made. Be sure to pass in the list of trades provided by the 'quality_agent'.
        6. After you receive the final report from the 'report_generator_agent', say "TERMINATE". If you don't receive the final report then ask 'report_generator_agent' again.
        
        You must adhere to the following rules:
        * Follow the rules step by step.
        * You never fabricate information. You base your reporting on factual information in your findings.
        * Only suggest a function call to 'query_db'. Never suggest a function call to anything else.
        * Final report from 'report_generator_agent' must be produced before saying 'TERMINATE'.

        """,
        human_input_mode="NEVER",
        max_consecutive_auto_reply=20,  # This condition triggers termination if the number of automatic responses to the same sender exceeds this threshold. ex: manager_agent --> chat_manager more than 10 times will trigger termination.
        code_execution_config=False,
        llm_config={
            "config_list": config_list,
            "functions": [query_db_declaration],
        },
        description="A manager that coordinates work among various agents.",
        is_termination_msg=is_termination_msg,
    )

    hypothesis_agent = autogen.AssistantAgent(
        name="hypothesis_agent",
        system_message="""
        You are a world-class stock strategy developer, capable of developing strategies that significantly outperform the general stock market. Your goal is to propose and modify strategies to give increased risk adjusted annual returns.

        You must adhere to the following rules:
        * You never fabricate information. You base your reporting on factual information in your findings.
        """,
        human_input_mode="NEVER",
        code_execution_config=False,
        description="A hypothesis agent that proposes and refines investing strategies.",
        is_termination_msg=is_termination_msg,
    )


    query_agent = autogen.AssistantAgent(
        name="query_agent",
        system_message="""
        You are a world-class data aggregator for data related to stock trading, capable of querying any data necessary to validate a strategy. Your goal is to put together the required data to test a stock trading hypothesis. If you don't know the schema or data available, query the DB first to get this information. Use the 'query_db' function to make any necessary db calls. 

        You must adhere to the following rules:
        * You never fabricate information. You base your reporting on factual information in your findings.
        """,
        human_input_mode="NEVER",
        code_execution_config=False,
        function_map={
            "query_db": query_db,
        },
        description="A query agent that finds the data necessary to test stock trading strategies.",
        is_termination_msg=is_termination_msg,
    )

    quality_agent = autogen.AssistantAgent(
        name="quality_agent",
        system_message="""
        You are a stringent stock market strategy investing supervisor. Your goal is to validate how well a proposed stock strategy works given the data provided. Return a list of all trades.
         
        You must adhere to the following rules:
        * You never fabricate information. You base your reporting on factual information in your findings.
        """,
        human_input_mode="NEVER",
        code_execution_config=False,
        description="Quality assurance agent that assesses stock market trading strategies.",
        is_termination_msg=is_termination_msg,
    )

    report_generator_agent = autogen.AssistantAgent(
        name="report_generator_agent",
        system_message="""
        You are a stock strategy reporter. The report should include the overall ROI and a list of all trades executed. It should include nothing else.

        You must adhere to the following rules:
        * You never fabricate information. You base your reporting on factual information in your findings.
        """,
        human_input_mode="NEVER",
        description="A world class stock trading strategy research reporter.",
        is_termination_msg=is_termination_msg,
    )

    def get_agent_of_name(agents, name):
        for agent in agents:
            if agent.name == name:
                return agent
        return None

    # ------------------ State transition for hub and spoke model with manager at the center ------------------ #
    agents = [manager_agent, query_agent, quality_agent, report_generator_agent, hypothesis_agent]

    allowed_speaker_transitions = {
        get_agent_of_name(agents, "manager_agent"): [
            get_agent_of_name(agents, "hypothesis_agent"),
            get_agent_of_name(agents, "quality_agent"),
            get_agent_of_name(agents, "report_generator_agent"),
            get_agent_of_name(agents, "query_agent")
        ],
        get_agent_of_name(agents, "hypothesis_agent"): [get_agent_of_name(agents, "manager_agent")],
        get_agent_of_name(agents, "quality_agent"): [get_agent_of_name(agents, "manager_agent")],
        get_agent_of_name(agents, "report_generator_agent"): [
            get_agent_of_name(agents, "manager_agent")
        ],
        get_agent_of_name(agents, "query_agent"): [
            get_agent_of_name(agents, "manager_agent")
        ],
    }

    # Initialize group chat and manager
    groupchat = autogen.GroupChat(
        agents=agents,
        messages=[],
        allowed_or_disallowed_speaker_transitions=allowed_speaker_transitions,
        speaker_transitions_type="allowed",
        max_round=10,
    )

    group_chat_manager = autogen.GroupChatManager(
        groupchat=groupchat,
        llm_config={"config_list": config_list},
        code_execution_config=False,
        is_termination_msg=is_termination_msg,
    )

    message = """
      Start the research.
      """
    await manager_agent.a_initiate_chat(
        group_chat_manager, message=message, max_turns=20, summary_method="reflection_with_llm"
    )


# Entry point for the script
if __name__ == "__main__":
    asyncio.run(stock_research())
