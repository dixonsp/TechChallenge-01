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
def scraping(tipo:eTipo=None, anoInicio:int=2022, anoTermino:int=2024):
    scraping = WebScraping(anoInicio=anoInicio, anoTermino=anoTermino, tipo=None)
    scraping.WebScaping(tipo=None ,anoInicio=anoInicio, anoTermino=anoTermino)