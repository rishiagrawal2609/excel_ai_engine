import uvicorn
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
import shutil
import os


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
    try:
        upload_file_path = os.path.join(UPLOAD_DIRECTORY, "file1.xlsx")

        with open(upload_file_path, "wb") as buffer:
            shutil.copyfileobj(excel_file.file, buffer)

        global df, df_unstruct
        df = pd.read_excel(upload_file_path)
        df_unstruct = pd.read_excel(upload_file_path, sheet_name='Unstructured_Data')

        return {
            "message": "File uploaded successfully",
            "length": df.shape[0],
            "length_unstruct": df_unstruct.shape[0]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while uploading the file: {str(e)}")

@app.post("/upload-second")
async def upload_second_excel_file(excel_file: UploadFile = File(...)):
    '''
    This function is used to upload the excel file for the operations like join

    Args:
    excel_file: The excel file to be uploaded

    Returns:
    dict: The response message and the number of rows in the uploaded file
    '''
    try:
        upload_file_path = os.path.join(UPLOAD_DIRECTORY, "file2.xlsx")

        with open(upload_file_path, "wb") as buffer:
            shutil.copyfileobj(excel_file.file, buffer)

        global df_2
        df_2 = pd.read_excel(upload_file_path)

        return {
            "message": "File uploaded successfully",
            "length": df_2.shape[0],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while uploading the file: {str(e)}")

@app.post("/query")
def query_by_user(user_input: str = Form(...)):
    '''
    This function is used to get the user input
    
    Args:
    user_input: The user input

    Returns:
    dict: The user input
    '''
    try:
        return {"user_input": user_input}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.post("/operate")
def operate(user_input: str = Form(...)):
    '''
    This function is used to get the user input and return the response

    Args:
    user_input: The user input

    Returns:
    dict: The response of the operation that user has requested
    '''
    try:
        response = get_operation(df, user_input)
        print(response)
        out = execute_llm_function(df, response)
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.post("/operate-unstruct")
def operate_unstruct(user_input: str = Form(...)):
    '''
    This function is used to get the user input and return the response

    Args:
    user_input: The user input

    Returns:
    dict: The response of the operation that user has requested
    '''
    try:
        df_unstruct = pd.read_excel("./uploads/file1.xlsx", sheet_name='Unstructured_Data')
        response = get_operation(df_unstruct, user_input)
        print(response)
        out = execute_llm_function(df_unstruct, response)
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)