import streamlit as st

def require_login():
    if "usuario" not in st.session_state or st.session_state.usuario is None:
        st.warning("🔒 Você precisa estar logado.")
        st.stop()

def require_admin():
    require_login()
    if st.session_state.usuario["perfil"] != "admin":
        st.warning("🔒 Acesso restrito a administradores.")
        st.stop()

def require_usuario():
    require_login()
    if st.session_state.usuario["perfil"] not in ["admin", "usuario"]:
        st.warning("🔒 Acesso restrito.")
        st.stop()