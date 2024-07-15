from fastapi import FastAPI
from fastapi import HTTPException

app = FastAPI()

@app.get("/")
def root():
    return {'TechChallenge 01':'WEB Scraping, Spark, RDBMS, API, JWT, AWS, Data Lake'}

@app.get("/scraping")
def scraping():
    

    return {'TechChallenge 01':'FastAPI, JWT, AWS'}