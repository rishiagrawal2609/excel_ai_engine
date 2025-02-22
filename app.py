import uvicorn
import pandas as pd
import numpy as np
from fastapi.responses import JSONResponse
from fastapi import FastAPI, UploadFile, File, Form
import shutil
import os
from typing import List

app = FastAPI()


UPLOAD_DIRECTORY = "./uploads"

#Creating the dynamic directory
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True) # For saving the uploaded files

#Creating the DataFrame for the data manupulation
df = pd.DataFrame()



@app.get("/")
def home():
    return {"data": "Fast API works"}

# Ensure directories exist
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True)

@app.post("/upload")
async def upload_excel_file(excel_file: UploadFile = File(...)):

    upload_file_path = os.path.join(UPLOAD_DIRECTORY, excel_file.filename) 

    with open(upload_file_path, "wb") as buffer:
        shutil.copyfileobj(excel_file.file, buffer)

    df = pd.read_excel(upload_file_path)

    return {
        "message": "File uploaded successfully",
        "length": df.shape[0]
    }

@app.post("/upload-multiple-excel/")
async def upload_multiple_excel_files(excel_files: List[UploadFile] = File(...)):
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
    return {"user_input": user_input}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)