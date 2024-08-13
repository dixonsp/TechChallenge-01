# Importa as bibliotecas necessárias
import sqlite3  # Biblioteca para interagir com o banco de dados SQLite
from fastapi import FastAPI, HTTPException, status, Depends  # Importa os módulos FastAPI para criar a API e lidar com exceções e status HTTP
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm  # Importa módulos de segurança para autenticação OAuth2
import uvicorn  # Servidor ASGI para rodar a aplicação FastAPI
import jwt  # Biblioteca para criar e verificar tokens JWT
from jwt import InvalidTokenError  # Importa exceção para tokens JWT inválidos
from jwt.exceptions import InvalidTokenError  # Importa exceção para tokens JWT inválidos (duas formas de importação)
from jose import jwt  # Outra biblioteca para lidar com JWT
from passlib.context import CryptContext  # Biblioteca para hashing e verificação de senhas
from pydantic import BaseModel  # Biblioteca para validação de dados e criação de modelos
from typing import Annotated  # Biblioteca para criar tipos anotados
import streamlit as st  # Biblioteca para criar dashboards interativos
from datetime import datetime, timedelta, timezone  # Importa módulos para manipulação de datas e horários
from Utils.webscraping import WebScraping  # Importa uma classe personalizada para web scraping
from model.negocio import eTipo  # Importa um enum personalizado para tipos de negócios
import pandas as pd  # Biblioteca para manipulação de dados em DataFrames
import json  # Biblioteca para manipulação de dados JSON

# Abre o arquivo de configurações para pegar parâmetros de criptografia
with open("config/parametros.json", "r") as file:  # Cria um contexto 'with' para abrir o arquivo de parâmetros
    data = json.load(file)  # Carrega os dados do arquivo JSON
    SECRET_KEY = data['CHAVE_SECRETA']  # Obtém a chave secreta para criptografia de tokens
    ALGORITHM = data['ALGORITHM']  # Obtém o algoritmo usado para criptografia de tokens
    ACCESS_TOKEN_EXPIRE_MINUTES = data["ACCESS_TOKEN_EXPIRE_MINUTES"]  # Obtém o tempo de expiração dos tokens

# Cria uma variável com a data e hora de execução
agora = datetime.now()  # Obtém a data e hora atuais
data_hora_formatada = agora.strftime("%Y-%m-%d %H:%M:%S")  # Formata a data e hora para um formato legível

# Classe que representa o Token de autenticação
class Token(BaseModel):
    """Modelo para representar um Token de autenticação."""
    access_token: str  # String do token de acesso
    token_type: str  # Tipo de token (Bearer)

# Classe que representa os dados do Token
class TokenData(BaseModel):
    """Modelo para representar os dados contidos no Token."""
    username: str | None = None  # Nome de usuário, pode ser None

# Classe que representa um usuário
class User(BaseModel):
    """Modelo que representa um usuário básico."""
    username: str  # Nome de usuário

# Classe que representa um usuário no banco de dados
class UserInDB(User):
    """Modelo que representa um usuário armazenado no banco de dados, incluindo senha."""
    senha: str  # Senha do usuário

# Configuração do contexto de criptografia para senhas
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")  # Define esquema de criptografia de senha

# Configuração do esquema OAuth2 para autenticação via token
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")  # URL para obtenção do token

# Criação da aplicação FastAPI
app = FastAPI()

# Função para verificar a senha
def verify_password(plain_password, hashed_password):
    """
    Verifica se a senha fornecida corresponde à senha armazenada (hash).
    
    :param plain_password: Senha fornecida pelo usuário.
    :param hashed_password: Senha armazenada (hash).
    :return: True se a senha for válida, False caso contrário.
    """
    return pwd_context.verify(plain_password, hashed_password)  # Verifica a senha utilizando o contexto de criptografia

# Função para gerar o hash de uma senha
def get_password_hash(password):
    """
    Gera o hash de uma senha.
    
    :param password: Senha em texto plano.
    :return: Hash da senha.
    """
    return pwd_context.hash(password)  # Gera e retorna o hash da senha

# Função para buscar um usuário no banco de dados
def get_user(username: str):
    """
    Busca um usuário no banco de dados pelo nome de usuário.
    
    :param username: Nome de usuário a ser buscado.
    :return: Objeto UserInDB se o usuário for encontrado, None caso contrário.
    """
    print("Conectando ao banco de dados")
    conn = sqlite3.connect('data/techchallenge01.db')  # Conecta ao banco de dados SQLite
    cursor = conn.cursor()  # Cria um cursor para executar comandos SQL

    cursor.execute("SELECT username, senha FROM usuario WHERE username = ?", (username,))  # Executa consulta para buscar o usuário
    resultados = cursor.fetchall()  # Obtém os resultados da consulta

    for linha in resultados:  # Itera sobre os resultados
        user_dict = pd.Series(linha, index=["username", "senha"]).to_dict()  # Converte os resultados em um dicionário
        return UserInDB(**user_dict)  # Retorna o usuário como um objeto UserInDB

    conn.close()  # Fecha a conexão com o banco de dados

# Função para autenticar um usuário
def authenticate_user(username: str, password: str):
    """
    Autentica um usuário verificando sua senha.
    
    :param username: Nome de usuário.
    :param password: Senha fornecida.
    :return: Objeto UserInDB se a autenticação for bem-sucedida, False caso contrário.
    """
    print(f'Iniciando autenticação: {username}')
    user = get_user(username)  # Busca o usuário no banco de dados
    if not user:  # Verifica se o usuário não foi encontrado
        print('Autenticação: NEGADA')
        st.session_state.jwt_token = None  # Reseta o token JWT na sessão
        return False
    if not verify_password(password, user.senha):  # Verifica se a senha está incorreta
        print('Autenticação: NEGADA')
        return False
    return user  # Retorna o usuário se a autenticação for bem-sucedida

# Função para criar um token de acesso JWT
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    """
    Cria um token de acesso JWT.
    
    :param data: Dados que serão codificados no token.
    :param expires_delta: Tempo para expiração do token.
    :return: Token JWT codificado.
    """
    to_encode = data.copy()  # Copia os dados para codificação
    if expires_delta:  # Verifica se foi especificado um tempo de expiração
        expire = datetime.now(timezone.utc) + expires_delta  # Define o tempo de expiração
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=30)  # Define tempo de expiração padrão de 30 minutos
    to_encode.update({"exp": expire})  # Adiciona a expiração aos dados
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)  # Codifica os dados em um token JWT
    return encoded_jwt  # Retorna o token codificado

# Função assíncrona para obter o usuário atual a partir do token JWT
async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    """
    Obtém o usuário atual a partir do token JWT.
    
    :param token: Token JWT fornecido na requisição.
    :return: Objeto UserInDB se o token for válido, lança HTTPException caso contrário.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])  # Decodifica o token JWT
        username: str = payload.get("sub")  # Obtém o nome de usuário do payload do token
        if username is None:  # Verifica se o nome de usuário está ausente
            raise credentials_exception
        token_data = TokenData(username=username)  # Cria um objeto TokenData com o nome de usuário
    except InvalidTokenError:  # Captura exceção de token inválido
        raise credentials_exception
    user = get_user(username=token_data.username)  # Busca o usuário no banco de dados
    if user is None:  # Verifica se o usuário não foi encontrado
        raise credentials_exception
    return user  # Retorna o usuário autenticado

# Função para cadastrar um novo usuário no banco de dados
def CadastrarUsuario(username:str, senha:str):
    """
    Cadastra um novo usuário no banco de dados com a senha hash.
    
    :param username: Nome de usuário a ser cadastrado.
    :param senha: Senha em texto plano.
    """
    hash_senha = get_password_hash(senha)  # Gera o hash da senha
    try:
        conn = sqlite3.connect('data/techchallenge01.db')  # Conecta ao banco de dados SQLite
        cursor = conn.cursor()  # Cria um cursor para executar comandos SQL

        cursor.execute("INSERT INTO usuario (username, senha) VALUES(?,?)", (username, hash_senha))  # Executa a inserção do novo usuário
        conn.commit()  # Confirma as alterações no banco de dados
        conn.close()  # Fecha a conexão com o banco de dados
    except Exception as e:
        print(f"Erro ao cadastrar usuário: {e}")  # Imprime a mensagem de erro caso ocorra uma exceção

# Rota para login e obtenção de token de acesso
@app.post("/token")
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()],) -> Token:
    """
    Endpoint para login e obtenção de um token de acesso.
    
    :param form_data: Dados de login fornecidos pelo formulário.
    :return: Token JWT de acesso se a autenticação for bem-sucedida.
    """
    user = authenticate_user(form_data.username, form_data.password)  # Autentica o usuário
    if not user:  # Verifica se a autenticação falhou
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=30)  # Define o tempo de expiração do token
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )  # Cria o token de acesso
    print(f"Access Token: {access_token}")  # Imprime o token de acesso gerado
    st.session_state.jwt_token = access_token  # Armazena o token JWT na sessão
    return Token(access_token=access_token, token_type="bearer")  # Retorna o token de acesso no formato esperado

# Rota para cadastrar um novo usuário
@app.post("/CadastrarUsuario")
def cadastro(token: Annotated[str, Depends(oauth2_scheme)], username:str, senha:str):
    """
    Endpoint para cadastrar um novo usuário.
    
    :param token: Token JWT para autenticação.
    :param username: Nome de usuário a ser cadastrado.
    :param senha: Senha em texto plano.
    """
    cadastro = CadastrarUsuario(username, senha)  # Chama a função de cadastro de usuário
    return  # Retorna vazio (apenas confirma a operação)

# Rota para realizar web scraping de produção
@app.get("/WebScrapingProducao")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    """
    Endpoint para realizar web scraping de dados de produção.
    
    :param token: Token JWT para autenticação.
    :param persistHtml: Define se o HTML será persistido.
    :param persistParquet: Define se os dados serão persistidos em formato Parquet.
    :param lerHTML_SemScraping: Define se o HTML será lido sem scraping.
    :return: Dados JSON obtidos pelo web scraping.
    """
    scraping = WebScraping(tipo=eTipo.PRODUCAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Inicializa o web scraping
    json_retorno = scraping.WebScaping(tipo=eTipo.PRODUCAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Executa o web scraping
    return json_retorno  # Retorna os dados obtidos em formato JSON

# Rota para realizar web scraping de dados de processamento
@app.get("/WebScrapingProcessamento")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    """
    Endpoint para realizar web scraping de dados de processamento.
    
    :param token: Token JWT para autenticação.
    :param persistHtml: Define se o HTML será persistido.
    :param persistParquet: Define se os dados serão persistidos em formato Parquet.
    :param lerHTML_SemScraping: Define se o HTML será lido sem scraping.
    :return: Dados JSON obtidos pelo web scraping.
    """
    scraping = WebScraping(tipo=eTipo.PROCESSAMENTO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Inicializa o web scraping
    json_retorno = scraping.WebScaping(tipo=eTipo.PROCESSAMENTO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Executa o web scraping
    return json_retorno  # Retorna os dados obtidos em formato JSON

# Rota para realizar web scraping de dados de comercialização
@app.get("/WebScrapingComercializacao")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    """
    Endpoint para realizar web scraping de dados de comercialização.
    
    :param token: Token JWT para autenticação.
    :param persistHtml: Define se o HTML será persistido.
    :param persistParquet: Define se os dados serão persistidos em formato Parquet.
    :param lerHTML_SemScraping: Define se o HTML será lido sem scraping.
    :return: Dados JSON obtidos pelo web scraping.
    """
    scraping = WebScraping(tipo=eTipo.COMERCIALIZACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Inicializa o web scraping
    json_retorno = scraping.WebScaping(tipo=eTipo.COMERCIALIZACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Executa o web scraping
    return json_retorno  # Retorna os dados obtidos em formato JSON

# Rota para realizar web scraping de dados de importação
@app.get("/WebScrapingImportacao")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    """
    Endpoint para realizar web scraping de dados de importação.
    
    :param token: Token JWT para autenticação.
    :param persistHtml: Define se o HTML será persistido.
    :param persistParquet: Define se os dados serão persistidos em formato Parquet.
    :param lerHTML_SemScraping: Define se o HTML será lido sem scraping.
    :return: Dados JSON obtidos pelo web scraping.
    """
    scraping = WebScraping(tipo=eTipo.IMPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Inicializa o web scraping
    json_retorno = scraping.WebScaping(tipo=eTipo.IMPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Executa o web scraping
    return json_retorno  # Retorna os dados obtidos em formato JSON

# Rota para realizar web scraping de dados de exportação
@app.get("/WebScrapingExportacao")
def scraping(token: Annotated[str, Depends(oauth2_scheme)], persistHtml:bool=True, persistParquet:bool=True, lerHTML_SemScraping:bool=True):
    """
    Endpoint para realizar web scraping de dados de exportação.
    
    :param token: Token JWT para autenticação.
    :param persistHtml: Define se o HTML será persistido.
    :param persistParquet: Define se os dados serão persistidos em formato Parquet.
    :param lerHTML_SemScraping: Define se o HTML será lido sem scraping.
    :return: Dados JSON obtidos pelo web scraping.
    """
    scraping = WebScraping(tipo=eTipo.EXPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Inicializa o web scraping
    json_retorno = scraping.WebScaping(tipo=eTipo.EXPORTACAO, persistHtml=persistHtml, persistParquet=persistParquet, lerHTML_SemScraping=lerHTML_SemScraping)  # Executa o web scraping
    return json_retorno  # Retorna os dados obtidos em formato JSON

# Ponto de entrada da aplicação
if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)  # Roda a aplicação FastAPI utilizando Uvicorn no localhost, porta 8000