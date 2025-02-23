import uvicorn
import pandas as pd
import numpy as np
from fastapi.responses import JSONResponse
from fastapi import FastAPI, UploadFile, File, Form
import shutil
import os
from typing import List
import excel_functions as ef

from query_parser import get_operation, execute_llm_function

app = FastAPI()

UPLOAD_DIRECTORY = "./uploads"

# Creating the dynamic directory
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True)  # For saving the uploaded files

# Creating the DataFrame for the data manipulation
df = pd.DataFrame()
df_unstruct = pd.DataFrame()
df_2 = pd.DataFrame() 

@app.get("/")
def home():
    return {"data": "Fast API works"}

@app.post("/upload")
async def upload_excel_file(excel_file: UploadFile = File(...)):
    '''
    This function is used to upload the excel file

    Args:
    excel_file: The excel file to be uploaded

    Returns:
    dict: The response message and the number of rows in the uploaded file
    '''

    upload_file_path = os.path.join(UPLOAD_DIRECTORY, excel_file.filename)

    with open(upload_file_path, "wb") as buffer:
        shutil.copyfileobj(excel_file.file, buffer)

    global df
    df = pd.read_excel(upload_file_path)
    df_unstruct = pd.read_excel(upload_file_path, sheet_name='Unstructured_Data')

    return {
        "message": "File uploaded successfully",
        "length": df.shape[0],
        "lenght_unstruct": df_unstruct.shape[0]
    }

@app.post("/upload-second")
async def upload_second_excel_file(excel_file: UploadFile = File(...)):
    '''
    This function is used to upload the excel file for the operations like join

    Args:
    excel_file: The excel file to be uploaded

    Returns:
    dict: The response message and the number of rows in the uploaded file
    '''
    upload_file_path = os.path.join(UPLOAD_DIRECTORY, excel_file.filename)

    with open(upload_file_path, "wb") as buffer:
        shutil.copyfileobj(excel_file.file, buffer)

    global df_2
    df_2 = pd.read_excel(upload_file_path)
    

    return {
        "message": "File uploaded successfully",
        "length": df_2.shape[0],
    }


@app.post("/query")
def query_by_user(user_input: str = Form(...)):
    '''
    This function is used to get the user input
    
    Args:
    user_input: The user input

    Returns:
    dict: The user input
    '''
    return {"user_input": user_input}

@app.post("/operate")
def operate(user_input: str = Form(...)):
    '''
    This function is used to get the user input and return the response

    Args:
    user_input: The user input

    Returns:
    dict: The response of the operation that user has requested
    '''

    response = get_operation(df,user_input)
    print(response)
    out = execute_llm_function(df, response)
    return {"result": out}


@app.post("/operate-unstruct")
def operate_unstruct(user_input: str = Form(...)):
    '''
    This function is used to get the user input and return the response

    Args:
    user_input: The user input

    Returns:
    dict: The response of the operation that user has requested
    '''
    response = get_operation(df_unstruct,user_input)
    print(response)
    out = execute_llm_function(df_unstruct, response)
    return {"result": out}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)