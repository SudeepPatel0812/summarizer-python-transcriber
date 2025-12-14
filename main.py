# Load env to containers
from dotenv import load_dotenv
load_dotenv()

# Start FastAPI Application
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}