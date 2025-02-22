from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage,HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
import json
from groq import Groq
import os
from dotenv import load_dotenv
import pandas as pd
\

import excel_functions as ef

load_dotenv()

groq_api_key = os.environ['GROQ_API_KEY']
model = 'llama3-8b-8192'

groq_chat = ChatGroq(
        groq_api_key=groq_api_key, 
        model_name=model
)

df = pd.DataFrame()
# for var_name, df in dfs.items():
#      input("Select the DF for the operation:")
#      print(f"\nDataFrame for {var_name}:")
#      df = dfs[var_name]
#      break
df = ef.dfs['read_df']


system_prompt = "You are a friendly chatbot. Users provide you query and you provide the intent of the query."

def get_intent(query):
    query_updated = query + f' Available Columns: {df.columns} What operation should be performed?' +  'Choose from: sum, average, min, max, filter, join, pivot, unpivot, sentiment, summarize, date difference. the output should be only the json do not provide any explaination. Expected Output: {"operation": "average", "column": "Salary", "filter": {"Department": "IT"}}'
    prompt = ChatPromptTemplate.from_messages(
        [
            SystemMessage(system_prompt),
            HumanMessage(query_updated),
        ]
    )

    chain = prompt | groq_chat

    result = groq_chat.invoke(query_updated)
    response = result.content
    response_json = json.loads(response)
    return response_json

response = get_intent("Find the average salary of employees in the finance department.")

print(response)
