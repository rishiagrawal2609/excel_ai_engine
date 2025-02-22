from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage,HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
import json
import os
from dotenv import load_dotenv
import pandas as pd
import excel_functions
import excel_functions as ef

load_dotenv()

groq_api_key = os.environ['GROQ_API_KEY']
model = 'llama3-8b-8192'

groq_chat = ChatGroq(
        groq_api_key=groq_api_key, 
        model_name=model
)



system_prompt = '''You are a friendly chatbot. Users provide you query and you provide the intent of the query.

I have a dataset and need to perform operations using Pandas.

Based on the available operations, return only structured JSON containing:
- `operation`: The function that should be called (e.g., `avg_with_filter`).
- `column`: The relevant column name(s).
- `filter`: Any filtering condition needed.

Use the following function reference to determine the correct mapping:

    The following functions are available for structured data manipulation in Pandas. When processing user queries, extract intent and map to the most appropriate function.

Basic Math Operations  
   - `maths_operations_on_same_col(action, df, input_column_name)`:  
     - Performs a mathematical operation (`add`, `subtract`, `multiply`, `divide`) on a single column and stores the result in a new column.  
     - Example: "Double the values in the 'Salary' column."

   - `maths_operations_on_diff_cols(action, df, column1, column2)`:  
     - Performs a mathematical operation (`add`, `subtract`, `multiply`, `divide`) between two different columns.  
     - Example: "Calculate profit by subtracting 'Cost' from 'Revenue'."

Aggregation Functions**  
   - `calculate_summary_report(df)`:  
     - Computes sum, average, min, and max for all numerical columns.  
     - Example: "Show me the summary statistics of all numerical fields."

   - `sum_with_filter(df, column_name, value)`:  
     - Filters the DataFrame based on a column value and calculates the sum.  
     - Example: "Find the total sales in the IT department."

   - `avg_with_filter(df, column_name, value)`:  
     - Filters the DataFrame based on a column value and calculates the average.  
     - Example: "Find the average salary of Finance employees."

   - `total_avg(df, column_name)`:  
     - Computes the overall average of a numerical column.  
     - Example: "What is the average revenue across all departments?"


Data Manipulation & Filtering
   - `filter_data(df, column_name, value, dropna=True)`:  
     - Filters rows where a specific column matches a given value.  
     - Example: "Show all employees in the HR department."

   - `join_datasets(df1, df2, join_type='inner', on=None)`:  
     - Joins two datasets using inner, left, right, or outer joins.  
     - Example: "Merge employee details with salary records on 'EmployeeID'."

   - `pivot_table(df, index, columns, values, aggfunc='sum')`:  
     - Creates a pivot table summarizing data.  
     - Example: "Pivot table showing total revenue per department per year."

   - `unpivot_table(df, value_vars, var_name='variable', value_name='value')`:  
     - Converts a pivot table back into a normal dataset.  
     - Example: "Unpivot the quarterly sales columns back into a single column."

Date-Based Operations
   - `date_operations(df, date_column)`:  
     - Extracts year, month, and day from a date column.  
     - Example: "Extract the year and month from 'JoiningDate'."

   - `date_difference(df, start_date_column, end_date_column, result_column_name)`:  
     - Calculates the difference in days between two date columns.  
     - Example: "Calculate the number of days between 'StartDate' and 'EndDate'."



Respond with the function call only, no explaination, no markdown, in plain text.

'''



def get_intent(df,query):
    '''
    Description: This function is used to get the intent of the query

    Args:
    query: The query provided by the user

    Returns:
    response_json: The response json containing the intent
    '''
    
    query_updated = query + f' Available Columns: {df.columns} What operation should be performed?' +  'Choose from: sum, average, min, max, filter, join, pivot, unpivot, sentiment, summarize, date difference. the output should be only the json do not provide any explaination. Expected Output: {"operation": "average", "column": "Salary", "filter": {"Department": "IT"}}'

    result = groq_chat.invoke(query_updated)
    response = result.content
    response_json = json.loads(response)
    return response_json



def get_operation(df,query):
    '''
    Description: This function is used to get the operation to be performed
    
    Args:
    query: The query provided by the user
    
    Returns:
    response: The response of the operation that user has requested
    '''
    

    intent = get_intent(df,query)
    quey_updated = query + f' Available Columns: {df.columns}'

    result = groq_chat.invoke(system_prompt + 'User Query: '+quey_updated)
    response = result.content
    return response


FUNCTION_MAP = {
    name: func for name, func in vars(excel_functions).items() if callable(func)
}





def execute_llm_function(df, function_call_str):
    """
    Executes a function dynamically from an LLM-generated function call.

    Args:
    df (pd.DataFrame): The dataset.
    function_call_str (str): The Python function call as a string.

    Returns:
    result: The output of the executed function.
    """

    # Ensure df is available in the execution context
    local_vars = {"df": df, **FUNCTION_MAP}

    try:
        # Execute the function dynamically and store result
        result = eval(function_call_str, {}, local_vars)
        if isinstance(result, pd.DataFrame):
            return result.to_dict(orient="records")
        return result
    except Exception as e:
        return {"error": f"Failed to execute function: {str(e)}"}
