import os
import sys
from model.negocio import eTipo

root = os.path.abspath("./Utils")
sys.path.append(root)

from fastapi import FastAPI
from fastapi import HTTPException
from Utils.webscraping import WebScraping


app = FastAPI()

@app.get("/")
def root():
    return {'TechChallenge 01':'WEB Scraping, Spark, RDBMS, API, JWT, AWS, Data Lake'}

@app.get("/scraping")
def scraping(anoInicio:int=None, anoTermino:int=None, tipo:eTipo=None):
    scraping = WebScraping(anoInicio=anoInicio, anoTermino=anoTermino)
    scraping.WebScaping()

    return {'TechChallenge 01':'FastAPI, JWT, AWS'}