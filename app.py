import uvicorn
import pandas as pd
import numpy as np
from fastapi.responses import JSONResponse
from fastapi import FastAPI, UploadFile, File
import shutil
import os


UPLOAD_DIRECTORY = "./uploads"

#Creating the dynamic directory
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True) # For saving the uploaded files


app = FastAPI()

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

    return {
        "message": "File uploaded successfully"
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)