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
    return {'TechChallenge 01':'WEB Scraping, Spark, Parquet, API, JWT'}

@app.get("/WebScrapingProduto")
def scraping(persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    #logger.debug("LOG:INICIANDO WebScraping")
    scraping = WebScraping(tipo=eTipo.PRODUCAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    scraping.WebScaping(tipo=eTipo.PRODUCAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    #logger.debug("LOG:WebScraping FINALIZADO")

@app.get("/WebScrapingProcessamento")
def scraping(persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    scraping = WebScraping(tipo=eTipo.PROCESSAMENTO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    scraping.WebScaping(tipo=eTipo.PROCESSAMENTO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)

@app.get("/WebScrapingComercializacao")
def scraping(persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    scraping = WebScraping(tipo=eTipo.COMERCIALIZACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    scraping.WebScaping(tipo=eTipo.COMERCIALIZACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)

@app.get("/WebScrapingImportacao")
def scraping(persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    scraping = WebScraping(tipo=eTipo.IMPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    scraping.WebScaping(tipo=eTipo.IMPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)

@app.get("/WebScrapingExportacao")
def scraping(persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    scraping = WebScraping(tipo=eTipo.EXPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    scraping.WebScaping(tipo=eTipo.EXPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
