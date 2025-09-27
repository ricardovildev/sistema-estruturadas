import streamlit as st
from frontend.usuarios import cadastrar_usuario, usuario_existe

def render():
    if "usuario" not in st.session_state or st.session_state.usuario is None:
        st.warning("🔒 Você precisa estar logado.")
        st.stop()

    usuario = st.session_state.usuario
    if usuario["perfil"] not in ["admin", "usuario"]:
        st.warning("🔒 Acesso restrito.")
        st.stop()
   


    # 🧾 Formulário de cadastro
    st.title("👥 Cadastro de Usuários")
    
    nome = st.text_input("Nome completo")
    username = st.text_input("Nome de usuário")
    email = st.text_input("Email")
    senha = st.text_input("Senha", type="password")
    perfil = st.selectbox("Perfil", ["usuario", "admin"])
    
    if st.button("Cadastrar"):
        if not nome or not username or not email or not senha:
            st.warning("⚠️ Preencha todos os campos.")
        elif usuario_existe(username):
            st.error(f"❌ O usuário '{username}' já existe.")
        else:
            cadastrar_usuario(nome, username, senha, email, perfil)
            st.success(f"✅ Usuário '{username}' cadastrado com sucesso.")
