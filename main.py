from fastapi import FastAPI, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import RedirectResponse
import uvicorn
import jwt
from jwt import InvalidTokenError
from jwt.exceptions import InvalidTokenError
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from typing import Annotated

from datetime import datetime, timedelta, timezone

from Utils.webscraping import WebScraping
import Utils
from model.negocio import eTipo

# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "3552af3ed9096acf5d5f7d75bae0a7a40d92abb9451790836f24ecdf2a978903"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

agora = datetime.now()
data_hora_formatada = agora.strftime("%Y-%m-%d %H:%M:%S")

fake_users_db = {
    "johndoe": {
        "username": "johndoe",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": "fakehashedsecret",
        "disabled": False,
    },
    "dixon": {
        "username": "dixon",
        "full_name": "Adilson Hideki Yamaguchi",
        "email": "dixonsp@gmail.com",
        "hashed_password": "$2b$12$9kA2dOj47Hjsc6ldnA2LguUqSL/5CLZebcJ8mR9tcD3jJ/QrU2pw6",
        "disabled": False,
    },
    "alice": {
        "username": "alice",
        "full_name": "Alice Wonderson",
        "email": "alice@example.com",
        "hashed_password": "fakehashedsecret",
        "disabled": True,
    },
}

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: str | None = None

class User(BaseModel):
    username: str
    email: str | None = None
    full_name: str | None = None
    disabled: bool | None = None

class UserInDB(User):
    hashed_password: str

#user_dict = {
#    "username":"dixon"
#    , "email":"dixonsp@gmail.com"
#    , "full_name":"Adilson Hideki Yamaguchi"
#    , "disabled":"False"
#    , "hashed_password":"dixon"
#}

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()

#def fake_hash_password(password: str):
#    return "fakehashed" + password

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

def authenticate_user(fake_db, username: str, password: str):
    user = get_user(fake_db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except InvalidTokenError:
        raise credentials_exception
    user = get_user(fake_users_db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)],
):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user

@app.post("/token")
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    print(f"Access Token: {access_token}")
    return Token(access_token=access_token, token_type="bearer")


@app.get("/users/me/", response_model=User)
async def read_users_me(
    current_user: Annotated[User, Depends(get_current_active_user)],
):
    return current_user


@app.get("/")
def root(token: Annotated[str, Depends(oauth2_scheme)], ):
    return {'TechChallenge 01':'WEB Scraping, Spark, Parquet, API, JWT'}

@app.get("/WebScrapingProduto")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    #logger.debug("LOG:INICIANDO WebScraping")
    scraping = WebScraping(tipo=eTipo.PRODUCAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    json_retorno = scraping.WebScaping(tipo=eTipo.PRODUCAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    return json_retorno
    #logger.debug("LOG:WebScraping FINALIZADO")

@app.get("/WebScrapingProcessamento")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    scraping = WebScraping(tipo=eTipo.PROCESSAMENTO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    json_retorno = scraping.WebScaping(tipo=eTipo.PROCESSAMENTO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    return json_retorno

@app.get("/WebScrapingComercializacao")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    scraping = WebScraping(tipo=eTipo.COMERCIALIZACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    json_retorno = scraping.WebScaping(tipo=eTipo.COMERCIALIZACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    return json_retorno

@app.get("/WebScrapingImportacao")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    scraping = WebScraping(tipo=eTipo.IMPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    json_retorno = scraping.WebScaping(tipo=eTipo.IMPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    return json_retorno

@app.get("/WebScrapingExportacao")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    scraping = WebScraping(tipo=eTipo.EXPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    json_retorno = scraping.WebScaping(tipo=eTipo.EXPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)
    return json_retorno

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
