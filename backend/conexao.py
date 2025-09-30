import streamlit as st
from sqlalchemy import create_engine
import ssl


def conectar():
    try:
        usuario = st.secrets["DBUSER"]
        senha = st.secrets["DBPASSWORD"]
        host = st.secrets["DBHOST"]
        porta = int(st.secrets["DBPORT"])
        banco = st.secrets["DBNAME"]
    except KeyError as e:
        st.error(f"A chave {e} está faltando em st.secrets! Verifique seu secrets.toml ou painel no Streamlit Cloud.")
        st.stop()

    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    url = f"mysql+pymysql://{usuario}:{senha}@{host}:{porta}/{banco}"
    from sqlalchemy import create_engine
    engine = create_engine(url, connect_args={'ssl': ssl_context})
    return engine
