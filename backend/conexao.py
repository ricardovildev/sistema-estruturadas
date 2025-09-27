from sqlalchemy import create_engine
import os
import ssl
from dotenv import load_dotenv

load_dotenv()

def conectar():
    usuario = os.getenv("DB_USER")
    senha = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    porta = os.getenv("DB_PORT", "3306")
    banco = os.getenv("DB_NAME")

    print("Conectando em:", host, banco)

    url = f"mysql+pymysql://{usuario}:{senha}@{host}:{porta}/{banco}"

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    engine = create_engine(
        url,
        connect_args={"ssl": ssl_context}
    )

    return engine