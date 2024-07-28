import os
import sys
from model.negocio import eTipo
from datetime import datetime, timedelta, timezone
import uvicorn
from jose import JWTError, jwt
#from jwt.exceptions import InvalidTokenError
from fastapi import FastAPI, Query, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import Annotated
#from passlib.context import CryptContext
#root = os.path.abspath("./Utils")
#sys.path.append(root)
from Utils.webscraping import WebScraping
from model import seguranca

agora = datetime.now()
data_hora_formatada = agora.strftime("%Y-%m-%d %H:%M:%S")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()

@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = seguranca.authenticate_user(seguranca.fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=seguranca.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = seguranca.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, seguranca.SECRET_KEY, algorithms=[seguranca.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = seguranca.TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = seguranca.fake_users_db.get(token_data.username)
    if user is None:
        raise credentials_exception
    return seguranca.UserInDB(**user)

@app.get("/users/me", response_model=seguranca.User)
async def read_users_me(current_user: seguranca.User = Depends(get_current_user)):
    return current_user

@app.get("/")
def root():
    return {'TechChallenge 01':'WEB Scraping, Spark, Parquet, API, JWT'}

@app.get("/WebScrapingProduto")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
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
