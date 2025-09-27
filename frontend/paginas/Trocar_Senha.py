import streamlit as st
import bcrypt
from sqlalchemy import text
from backend.importacao import engine

def render_trocar_senha():
    # Verifica se usuário está logado
    if "usuario" not in st.session_state or st.session_state.usuario is None:
        st.warning("🔒 Você precisa estar logado para trocar a senha.")
        st.stop()

    usuario = st.session_state.usuario
    usuario_id = usuario['id']  # Ajuste o campo conforme o seu dicionário de usuário

    st.title("🔐 Trocar Senha")

    senha_atual = st.text_input("Senha Atual", type="password")
    nova_senha = st.text_input("Nova Senha", type="password")
    confirma_senha = st.text_input("Confirme a Nova Senha", type="password")

    if st.button("Alterar senha"):
        if not senha_atual or not nova_senha or not confirma_senha:
            st.error("Todos os campos são obrigatórios.")
            return

        if nova_senha != confirma_senha:
            st.error("A nova senha e a confirmação não conferem.")
            return

        with engine.connect() as conn:
            resultado = conn.execute(
                text("SELECT senha_hash FROM usuarios WHERE id = :id"),
                {"id": usuario_id}
            ).fetchone()

            if resultado is None:
                st.error("Usuário não encontrado.")
                return

            senha_hash_armazenada = resultado["senha_hash"]

            # Verifica se a senha atual está correta
            if not bcrypt.checkpw(senha_atual.encode("utf-8"), senha_hash_armazenada.encode("utf-8")):
                st.error("Senha atual incorreta.")
                return

            # Cria hash para a nova senha
            nova_hash = bcrypt.hashpw(nova_senha.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

            # Atualiza senha no banco
            conn.execute(
                text("UPDATE usuarios SET senha_hash = :hash WHERE id = :id"),
                {"hash": nova_hash, "id": usuario_id}
            )
            conn.commit()

        st.success("Senha alterada com sucesso.")
