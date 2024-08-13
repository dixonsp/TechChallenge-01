# Importação das bibliotecas necessárias
import streamlit as st  # Biblioteca para criar interfaces de usuário interativas
import sqlite3  # Biblioteca para interagir com o banco de dados SQLite
import main  # Importa o módulo principal que contém as funções de autenticação
import json  # Biblioteca para manipulação de dados JSON
import jwt  # Biblioteca para criação e verificação de tokens JWT
import dashboard  # Importa o módulo do dashboard

# Carrega as configurações de criptografia a partir de um arquivo JSON
with open("config/parametros.json", "r") as file:  # Abre o arquivo de configuração JSON
    data = json.load(file)  # Carrega os dados do arquivo JSON
    CHAVE_SECRETA = data['CHAVE_SECRETA']  # Extrai a chave secreta para criptografia JWT
    ALGORITHM = data['ALGORITHM']  # Extrai o algoritmo de criptografia para JWT
    ACCESS_TOKEN_EXPIRE_MINUTES = data['ACCESS_TOKEN_EXPIRE_MINUTES']  # Extrai o tempo de expiração dos tokens

# Função para autenticar o usuário usando o banco de dados
def autenticar(usuario, senha):
    """
    Autentica o usuário verificando suas credenciais no banco de dados.

    :param usuario: Nome de usuário.
    :param senha: Senha fornecida.
    :return: Token JWT se a autenticação for bem-sucedida, None caso contrário.
    """
    return main.authenticate_user(usuario, senha)  # Chama a função de autenticação do módulo principal

# Função para registrar novos usuários
def registrar_usuario(usuario, senha):
    """
    Registra um novo usuário no banco de dados.

    :param usuario: Nome de usuário a ser registrado.
    :param senha: Senha em texto plano a ser armazenada.
    """
    hash_senha = main.get_password_hash(senha)  # Gera o hash da senha utilizando a função do módulo principal
    try:
        conn = sqlite3.connect('data/techchallenge01.db')  # Conecta ao banco de dados SQLite
        cursor = conn.cursor()  # Cria um cursor para executar comandos SQL
        username = usuario  # Atribui o nome de usuário

        # Executa a consulta SQL para inserir um novo usuário
        cursor.execute("INSERT INTO usuario (username, senha) VALUES(?,?)", (username, hash_senha))
        conn.commit()  # Confirma a transação
        conn.close()  # Fecha a conexão com o banco de dados

        st.success('Usuário registrado com sucesso!')  # Exibe mensagem de sucesso
    except Exception as e:
        st.error(f"Ocorreu um erro. {e}")  # Exibe mensagem de erro em caso de exceção

# Função para verificar a validade do token JWT
def verificar_token(token):
    """
    Verifica a validade de um token JWT.

    :param token: Token JWT a ser verificado.
    :return: Decodificação do token se válido, None se inválido ou expirado.
    """
    try:
        decodificado = jwt.decode(token, CHAVE_SECRETA, algorithms=ALGORITHM)  # Decodifica o token usando a chave secreta e o algoritmo
        return decodificado['user']  # Retorna o nome de usuário contido no token
    except jwt.ExpiredSignatureError:  # Captura exceção se o token estiver expirado
        return None
    except jwt.InvalidTokenError:  # Captura exceção se o token for inválido
        return None

# Função para exibir a tela de login e permitir o registro de novos usuários
def tela_login():
    """
    Exibe a tela de login e permite o registro de novos usuários.

    A função também trata a autenticação do usuário e armazena o token JWT na sessão se o login for bem-sucedido.
    """
    st.set_page_config(page_title='Login', page_icon='🔒')  # Configura a página do Streamlit
    st.title('Login')  # Define o título da página
    
    st.subheader('Por favor, entre com suas credenciais')  # Subtítulo da seção de login
    usuario = st.text_input('Usuário')  # Campo para entrada do nome de usuário
    senha = st.text_input('Senha', type='password')  # Campo para entrada da senha, tipo 'password' oculta o texto digitado

    if st.button('Login'):  # Botão para realizar o login
        token = autenticar(usuario, senha)  # Autentica o usuário e recebe o token JWT
        print(f'TOKEN: {token}')  # Imprime o token (para fins de depuração)
        if token:  # Verifica se o token foi retornado, indicando sucesso na autenticação
            st.session_state['token'] = token  # Armazena o token JWT na sessão do usuário
            st.session_state['logado'] = True  # Marca o estado de logado como True
            st.success('Login bem-sucedido!')  # Exibe mensagem de sucesso
        else:
            st.error('Credenciais inválidas. Tente novamente.')  # Exibe mensagem de erro se o login falhar