from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
import sqlite3
from Utils.utils import conectar_sqlite

password_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def cryptografar_senha(password: str) -> str:
    return password_context.hash(password)


def validar_senha(password: str, hashed_pass: str) -> bool:
    return password_context.verify(password, hashed_pass)

class Usuario(BaseModel):
    email: str
    senha: str

    def __init__(self, email:str, senha:str):
        super().__init__(email, senha)
        self.conn = conectar_sqlite("data/TechChallenge01.db")

    def cryptografar(cls, v):
        return cryptografar_senha(v)
    
    def validar(self, password:str) -> bool:
        return validar_senha(password, self.senha)

    def inserir(self, email:str, senha:str):
        with self.conn:
            self.conn.execute('INSERT INTO Usuario (email, senha) VALUES (?, ?)', (self.email, cryptografar_senha(self.senha)))

    def existe(self) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM Usuario WHERE email = ?', self.email)
        return cursor.fetchone() is not None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f"Tipo de exceção: {exc_type}")
            print(f"Valor da exceção: {exc_val}")
            print(f"Traceback da exceção: {exc_tb}")
        return False  # Re-raise exceptions, if any