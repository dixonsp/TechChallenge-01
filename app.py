# Importação das bibliotecas e módulos necessários
import streamlit as st  # Biblioteca para criar interfaces de usuário interativas
from auth import tela_login  # Importa a função tela_login do módulo auth
from dashboard import painel_dashboard  # Importa a função painel_dashboard do módulo dashboard
from auth import verificar_token  # Importa a função verificar_token do módulo auth

# Verificar se o usuário está logado
if 'token' in st.session_state:  # Verifica se há um token armazenado na sessão do usuário
    usuario = verificar_token(st.session_state['token'])  # Verifica a validade do token e recupera o usuário associado
    if usuario:  # Se o token for válido e o usuário for recuperado
        painel_dashboard()  # Exibe o painel do dashboard
    else:  # Se o token for inválido ou expirado
        st.session_state['logado'] = False  # Marca o usuário como não logado
        tela_login()  # Exibe a tela de login
else:
    tela_login()  # Se não houver token na sessão, exibe a tela de login