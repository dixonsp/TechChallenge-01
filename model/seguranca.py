from pydantic import BaseModel
from typing import Optional
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

ACCESS_TOKEN_EXPIRE_MINUTES = 30  # 30 minutos
REFRESH_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7 # 7 dias
ALGORITHM = "HS256" # algoritmo de cryptografia
SECRET_KEY = "TechChallenge01JWTSecret"   # secret key
REFRESH_SECRET_KEY = "TechChallenge01JWTSecretRefresh"    # secret key refresh

fake_users_db = {
    "testuser": {
        "username": "testuser",
        "hashed_password": "fakehashedpassword"
    }
}
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class User(BaseModel):
    username: str

class UserInDB(User):
    hashed_password: str

def fake_hash_password(password: str):
    return "fakehashed" + password

def authenticate_user(fake_db, username: str, password: str):
    user = fake_db.get(username)
    if not user:
        return False
    if user["hashed_password"] != fake_hash_password(password):
        return False
    return UserInDB(**user)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
