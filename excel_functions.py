# All the oprations that can be performed on excel files are written here for the structured data.

import pandas as pd
import numpy as np
import os
import re
from langchain_groq import ChatGroq
import json
import os
from fastapi import HTTPException

from dotenv import load_dotenv

load_dotenv()


groq_chat = ChatGroq(
    groq_api_key=os.getenv('GROQ_API_KEY'), 
    model_name='llama-3.3-70b-versatile'
)

'''
Supported Operations: The listed operations should be supported on the created Excel data. 
●	Basic Math Operations: Perform basic mathematical operations such as addition, subtraction, multiplication, and division on numerical columns. Create new columns to store the results.
●	Aggregations: Calculate aggregations like sum, average, min, max, etc., on numerical columns and produce a summary report.
●	Joining: Perform different types of joins (inner, left, right, etc.) with another dataset. Assume another dataset will be provided or generated.
●	Pivot and Unpivot: Create a pivot table from the existing data and also perform the reverse operation to unpivot the table back to a normal dataset.
●	Date Operations: Perform operations like extracting the month, day, and year from date columns, and calculate the difference between two dates.
'''

UPLOAD_DIRECTORY = "./uploads"


def variable_name_genrator(filename: str) -> str:
    '''
    Description: This function will generate a variable name from the filename.

    Args:
    filename (str): The name of the file

    Returns:
    str: The variable name generated from the filename        
    '''
    name = filename.replace(".xlsx", "").strip()
    name = re.sub(r'\W|^(?=\d)', '_', name) 
    return f"{name}_df"


def create_dfs_from_uploads():
    '''
    Description: This function will create DataFrames from the uploaded Excel files.
    
    Args:
    None (No input arguments) 
    
    Returns:
    dict: A dictionary containing the DataFrames created from the uploaded Excel files. The keys are the variable names of the DataFrames.
    '''
    dfs = {}
    if not os.path.exists(UPLOAD_DIRECTORY):
        print(f"Directory {UPLOAD_DIRECTORY} does not exist. Creating it now.")
        os.makedirs(UPLOAD_DIRECTORY)
        return dfs  

    for filename in os.listdir(UPLOAD_DIRECTORY):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(UPLOAD_DIRECTORY, filename)
            try:
                df = pd.read_excel(file_path)
                variable_name = variable_name_genrator(filename)
                dfs[variable_name] = df
                globals()[variable_name] = df  
                print(f"Created variable: {variable_name}")
            except Exception as e:
                print(f"Error loading {filename}: {e}")
    
    return dfs

dfs = create_dfs_from_uploads()
# for var_name, df in dfs.items():
#     print(f"\nDataFrame for {var_name}:")
#     print(df.head())



def maths_operations_on_same_col(action, df, input_column_name):
    '''
    Description: This function performs basic mathematical operations such as addition, subtraction, multiplication, and division on numerical columns of a DataFrame. It creates new columns to store the results.
    
    Args:
    action (str): The mathematical operation to perform. Choose from ['add', 'subtract', 'multiply', 'divide'].
    df (pd.DataFrame): The DataFrame containing the data.
    input_column_name (str): The name of the column on which the operation is to be performed.

    Returns:
    pd.DataFrame: The DataFrame with the new column added containing the result of the mathematical operation.
    '''
    if input_column_name not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{input_column_name}' not found in DataFrame.")

    result_col = f"{input_column_name}_{action}"

    if action == "add":
        df[result_col] = df[input_column_name] + df[input_column_name]
    elif action == "subtract":
        df[result_col] = df[input_column_name] - df[input_column_name]
    elif action == "multiply":
        df[result_col] = df[input_column_name] * df[input_column_name]
    elif action == "divide":
        if df[input_column_name].eq(0).any():
            raise HTTPException(status_code=400, detail="Cannot divide by zero.")
        df[result_col] = df[input_column_name] / df[input_column_name]
    else:
        raise HTTPException(status_code=400, detail="Invalid operation! Choose from ['add', 'subtract', 'multiply', 'divide']")
    
    return df


def maths_operations_on_diff_cols(action, df, column1, column2):
    '''
    Description: This function performs basic mathematical operations such as addition, subtraction, multiplication, and division on numerical columns of a DataFrame. It creates new columns to store the results.
    
    Args:
    action (str): The mathematical operation to perform. Choose from ['add', 'subtract', 'multiply', 'divide'].
    df (pd.DataFrame): The DataFrame containing the data.
    column1 (str): The name of the first column on which the operation is to be performed.
    column2 (str): The name of the second column on which the operation is to be performed.

    Returns:
    pd.DataFrame: The DataFrame with the new column added containing the result of the mathematical operation.
    '''
    if column1 not in df.columns or column2 not in df.columns:
        raise HTTPException(status_code=400, detail=f"One or both columns ('{column1}', '{column2}') not found in DataFrame.")

    result_col = f"{column1}_{action}_{column2}"

    if action == "add":
        df[result_col] = df[column1] + df[column2]
    elif action == "subtract":
        df[result_col] = df[column1] - df[column2]
    elif action == "multiply":
        df[result_col] = df[column1] * df[column2]
    elif action == "divide":
        if df[column2].eq(0).any():
            raise HTTPException(status_code=400, detail=f"Cannot divide by zero in column '{column2}'.")
        df[result_col] = df[column1] / df[column2]
    else:
        raise HTTPException(status_code=400, detail="Invalid operation! Choose from ['add', 'subtract', 'multiply', 'divide']")

    return df


def calculate_summary_report(df):
    '''
    Description: This function calculates aggregations like sum, average, min, max, etc., on numerical columns of a DataFrame and produces a summary report.

    Args:
    df (pd.DataFrame): The DataFrame containing the data.

    Returns:
    pd.DataFrame: A DataFrame containing the summary report with columns for sum, average, min, and max values.
    '''
    summary = {
        'sum': df.sum(numeric_only=True),
        'average': df.mean(numeric_only=True),
        'min': df.min(numeric_only=True),
        'max': df.max(numeric_only=True)
    }
    return pd.DataFrame(summary)

def join_datasets(df1, df2, join_type='inner', on=None):
    '''
    Description: This function performs different types of joins (inner, left, right, etc.) with another dataset.

    Args:
    df1 (pd.DataFrame): The first DataFrame.
    df2 (pd.DataFrame): The second DataFrame.
    join_type (str): The type of join to perform. Choose from ['inner', 'left', 'right', 'outer'].
    on (str or list): The column(s) to join the DataFrames on. If None, common columns will be used.

    Returns:
    pd.DataFrame: The DataFrame resulting from the join operation.
    '''
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise HTTPException(status_code=400, detail="Both inputs should be pandas DataFrames.")
    if on is None:
        common_cols = list(set(df1.columns) & set(df2.columns))
        if not common_cols:
            raise HTTPException(status_code=400, detail="No common columns found for joining. Specify 'on' manually.")
        on = common_cols  # Use common columns as default join keys
    
    return pd.merge(df1, df2, how=join_type, on=on)


def pivot_table(df, index, columns, values, aggfunc='sum'):
    '''
    Description: This function creates a pivot table from the existing data in a DataFrame.
    
    Args:
    df (pd.DataFrame): The DataFrame containing the data.
    index (str or list): The column(s) to use as index for the pivot table.
    columns (str or list): The column(s) to use as columns for the pivot table.
    values (str): The column to use as values for the pivot table.
    aggfunc (str or function): The aggregation function to use for the pivot table. Default is 'sum'.

    Returns:
    pd.DataFrame: The pivot table created from the DataFrame.
    '''

    if any(col not in df.columns for col in [index, columns, values]):
        raise HTTPException(status_code=400, detail="One or more specified columns are missing from the DataFrame.")
    
    return pd.pivot_table(df, index=index, columns=columns, values=values, aggfunc=aggfunc)

def unpivot_table(df, value_vars, var_name='variable', value_name='value'):
    '''
    Description: This function performs the reverse operation to unpivot a pivot table back to a normal dataset.
    
    Args:
    df (pd.DataFrame): The DataFrame containing the pivot table.
    value_vars (list): The columns to unpivot.
    var_name (str): The name of the new column to store the column names.
    value_name (str): The name of the new column to store the values.

    Returns:
    pd.DataFrame: The DataFrame resulting from the unpivot operation.
    '''

    if not all(var in df.columns for var in value_vars):
        raise HTTPException(status_code=400, detail="Some 'value_vars' are not found in the DataFrame.")
    
    return pd.melt(df, id_vars=[col for col in df.columns if col not in value_vars], 
                   value_vars=value_vars, var_name=var_name, value_name=value_name)


def date_operations(df, date_column):
    '''
    Description: This function performs operations like extracting the month, day, and year from date columns in a DataFrame.
    
    Args:
    df (pd.DataFrame): The DataFrame containing the data.
    date_column (str): The name of the date column.

    Returns:
    pd.DataFrame: The DataFrame with new columns added for year, month, and day extracted from the date column.
    '''
    if date_column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{date_column}' not found in DataFrame.")

    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df['year'] = df[date_column].dt.year
    df['month'] = df[date_column].dt.month
    df['day'] = df[date_column].dt.day
    return df


def date_difference(df, start_date_column, end_date_column, result_column_name):
    '''
    Description: This function calculates the difference between two dates in days and stores the result in a new column.

    Args:
    df (pd.DataFrame): The DataFrame containing the data.
    start_date_column (str): The name of the column containing the start date.
    end_date_column (str): The name of the column containing the end date.
    result_column_name (str): The name of the new column to store the result.

    Returns:
    pd.DataFrame: The DataFrame with the new column added containing the difference in days between the two dates.
    '''
    if start_date_column not in df.columns or end_date_column not in df.columns:
        raise HTTPException(status_code=400, detail="One or both date columns are missing in the DataFrame.")

    df[start_date_column] = pd.to_datetime(df[start_date_column], errors='coerce')
    df[end_date_column] = pd.to_datetime(df[end_date_column], errors='coerce')
    df[result_column_name] = (df[end_date_column] - df[start_date_column]).dt.days
    return df


def filter_data(df, column_name, value, dropna=True):
    '''
    Description: This function filters the data in a DataFrame based on a column value.

    Args:
    df (pd.DataFrame): The DataFrame containing the data.
    column_name (str): The name of the column to filter on.
    value: The value to filter on.

    Returns:
    pd.DataFrame: The filtered DataFrame.
    '''

    if column_name not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{column_name}' not found in DataFrame.")
    
    filtered_df = df[df[column_name] == value]
    
    if dropna:
        filtered_df = filtered_df.dropna()
    
    return filtered_df

def sum_with_filter(df, column_name, value):
    '''
    Description: This function calculates the sum of a column in a DataFrame after filtering based on a column value.

    Args:
    df (pd.DataFrame): The DataFrame containing the data.
    column_name (str): The name of the column to filter on.
    value: The value to filter on.

    Returns:
    float: The sum of the column after filtering.
    '''
    if column_name not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{column_name}' not found in DataFrame.")
    
    return df[df[column_name] == value].sum()

def avg_with_filter(df, column_name, value):
    '''
    Description: This function calculates the average of a column in a DataFrame after filtering based on a column value.

    Args:
    df (pd.DataFrame): The DataFrame containing the data.
    column_name (str): The name of the column to filter on.
    value: The value to filter on.

    Returns:
    float: The average of the column after filtering.
    '''
    if column_name not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{column_name}' not found in DataFrame.")
    
    return df[df[column_name] == value].mean(numeric_only=True)

def total_avg(df, column_name):
    '''
    Description: This function calculates the total average of a column in a DataFrame.

    Args:
    df (pd.DataFrame): The DataFrame containing the data.
    column_name (str): The name of the column to calculate the average of.

    Returns:
    float: The total average of the column.
    '''
    if column_name not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{column_name}' not found in DataFrame.")
    
    return df[column_name].mean(numeric_only=True)

MAX_CHAR_LIMIT = 23000

def truncate_text(text_list, max_chars=MAX_CHAR_LIMIT):
    combined_text = "\n".join(text_list)
    if len(combined_text) > max_chars:
        combined_text = combined_text[:max_chars] 
    return combined_text


def get_sentiment(df,text_column):
    '''
    Description: This function is used to get the sentiment of the text
    
    Args:
    text: The text for which sentiment needs to be calculated
    
    Returns:
    sentiment: The sentiment of the text
    '''
    if text_column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{text_column}' not found in the sheet.")
    text_data = df[text_column].dropna().astype(str).tolist()
    text_data = truncate_text(text_data)
    prompt = f"Analyze the sentiment of the following text entries:\n{text_data}\n"
    prompt += "Classify each entry as Positive, Negative, or Neutral."

    result = groq_chat.invoke(prompt)
    response = result.content
    return response