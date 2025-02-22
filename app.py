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

    return {
        "message": "File uploaded successfully",
        "length": df.shape[0]
    }

@app.post("/upload-multiple")
async def upload_multiple_excel_files(excel_files: List[UploadFile] = File(...)):
    '''
    This function is used to upload multiple excel files
    
    Args:
    excel_files: The list of excel files to be uploaded

    Returns:
    dict: The response message and the number of rows in each uploaded file
    '''
    results = []

    for file in excel_files:
        upload_file_path = os.path.join(UPLOAD_DIRECTORY, file.filename)

        with open(upload_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        df = pd.read_excel(upload_file_path)
        results.append({"filename": file.filename, "num_rows": df.shape[0]})

    return {"message": "Files uploaded successfully", "files": results}

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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)