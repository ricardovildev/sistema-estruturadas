from sqlalchemy import create_engine
import os

def conectar():
    usuario = os.getenv("DB_USER")
    senha = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    porta = os.getenv("DB_PORT", "3306")  # valor padrão
    banco = os.getenv("DB_NAME")
    
    url = f'mysql+pymysql://{usuario}:{senha}@{host}:{porta}/{banco}'
    engine = create_engine(url)
    return engine