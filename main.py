import os
import sys
from model.negocio import eTipo
import logging
from datetime import datetime
import uvicorn

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

agora = datetime.now()
data_hora_formatada = agora.strftime("%Y-%m-%d %H:%M:%S")


root = os.path.abspath("./Utils")
sys.path.append(root)

from fastapi import FastAPI, Query
from fastapi import HTTPException
from Utils.webscraping import WebScraping


app = FastAPI()

@app.get("/")
def root():
    return {'TechChallenge 01':'WEB Scraping, Spark, RDBMS, API, JWT, AWS, Data Lake'}

@app.get("/WebScrapingProduto")
def scraping(anoInicio:int=None, anoTermino:int=None, persistHtml:bool=True, persistParquet:bool=True):
    #logger.debug("LOG:INICIANDO WebScraping")
    scraping = WebScraping(tipo=eTipo.PRODUCAO, anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)
    scraping.WebScaping(tipo=eTipo.PRODUCAO ,anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)
    #logger.debug("LOG:WebScraping FINALIZADO")

@app.get("/WebScrapingProcessamento")
def scraping(anoInicio:int=None, anoTermino:int=None, persistHtml:bool=True, persistParquet:bool=True):
    scraping = WebScraping(tipo=eTipo.PROCESSAMENTO, anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)
    scraping.WebScaping(tipo=eTipo.PROCESSAMENTO ,anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)

@app.get("/WebScrapingComercializacao")
def scraping(anoInicio:int=None, anoTermino:int=None, persistHtml:bool=True, persistParquet:bool=True):
    scraping = WebScraping(tipo=eTipo.COMERCIALIZACAO, anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)
    scraping.WebScaping(tipo=eTipo.COMERCIALIZACAO ,anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)

@app.get("/WebScrapingImportacao")
def scraping(anoInicio:int=None, anoTermino:int=None, persistHtml:bool=True, persistParquet:bool=True):
    scraping = WebScraping(tipo=eTipo.IMPORTACAO, anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)
    scraping.WebScaping(tipo=eTipo.IMPORTACAO ,anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)

@app.get("/WebScrapingExportacao")
def scraping(anoInicio:int=None, anoTermino:int=None, persistHtml:bool=True, persistParquet:bool=True):
    scraping = WebScraping(tipo=eTipo.EXPORTACAO, anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)
    scraping.WebScaping(tipo=eTipo.EXPORTACAO ,anoInicio=anoInicio, anoTermino=anoTermino, persistHtml=persistHtml, persistParquet=persistParquet)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
