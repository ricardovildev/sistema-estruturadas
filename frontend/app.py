import streamlit as st
import pandas as pd
from datetime import date
import yfinance as yf
from st_aggrid import AgGrid, GridOptionsBuilder
import bcrypt
import sys
import os

# adiciona a raiz do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from backend.importacao import (
    importar_notas_atualizado,
    importar_historico_precos,
    importar_proventos,
    importar_ativos,
    calcular_resultado_opcoes,
    importar_vencimentos_opcoes,
    atualizar_historico_operacoes,
    obter_lista_assets,
    obter_preco_ultimo,
    atualizar_preco,
    importar_ativos_yahoo,
    atualizar_asset_yahoo,
    importar_ativos_livres,
)

from usuarios import autenticar_usuario
from paginas.Consulta_de_Premios import render as render_premios
from paginas.Ativos_Livres import render as render_ativos
from paginas.Consulta_de_Notas import render as render_consulta
from paginas.Posicao_Consolidada import render as render_posicao
from paginas.Cadastro_de_Usuarios import render as render_cadastro_usuario
from paginas.Trocar_Senha import render_trocar_senha
from admin_painel import render as render_admin
from paginas.Calculo_Estruturadas import render as render_estrutura
from dotenv import load_dotenv
load_dotenv()



# Configuração da página
st.set_page_config(page_title="Sistema Estruturadas", layout="wide")

# Inicializa sessão
if "usuario" not in st.session_state:
    st.session_state.usuario = None

# 🔐 Tela de login
if st.session_state.usuario is None:
    st.title("🔐 Login")

    username = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        usuario = autenticar_usuario(username, senha)
        if usuario:
            st.session_state.usuario = usuario
            st.success(f"Bem-vindo, {usuario['nome']}!")
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos.")

# 🔓 Conteúdo do sistema (após login)
else:
    usuario = st.session_state.usuario

    st.sidebar.markdown(f"👤 **Usuário:** {usuario['nome']}")
    st.sidebar.markdown(f"🔑 **Perfil:** {usuario['perfil']}")
    if st.sidebar.button("Sair"):
        st.session_state.usuario = None
        st.rerun()

    # 🔍 Menu personalizado por perfil
    if usuario["perfil"] == "admin":
        opcoes = [
            "Página inicial",
            "Consulta de Prêmios",
            "Ativos Livres",
            "Importações e Atualizações",
            "Consulta de Notas",
            "Consulta Posição",
            "Cadastro de Usuários",
            "Trocar Senha"
        ]
    else:
        opcoes = [
            "Página inicial",
            "Consulta de Prêmios",
            "Ativos Livres",
            "Consulta Posição",
            "Consulta de Notas",
            "Trocar Senha"
        ]

    pagina = st.sidebar.selectbox("📂 Navegação", opcoes)

    # 🔄 Conteúdo dinâmico por página
    if pagina == "Página inicial":
        st.title("📊 Sistema Estruturadas")
        st.success("Bem-vindo ao painel principal. Navegue pelo menu no canto esquerdo")

    elif pagina == "Consulta de Prêmios":
        render_premios()

    elif pagina == "Ativos Livres":
        render_ativos()

    elif pagina == "Importações e Atualizações" and usuario["perfil"] == "admin":
        render_admin()

    elif pagina == "Consulta de Notas":
        render_consulta()

    elif pagina == "Consulta Posição":
            render_posicao()

    elif pagina == "Cálculo Estruturadas":
    render_estrutura()
        
    elif pagina == "Cadastro de Usuários" and usuario["perfil"] == "admin":
            render_cadastro_usuario()
        
    elif pagina == "Trocar Senha":
        render_trocar_senha()



##################################








