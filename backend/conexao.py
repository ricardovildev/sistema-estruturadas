from sqlalchemy import create_engine

def conectar():
    usuario = 'root'
    senha = '7695'
    host = 'localhost'
    porta = '3306'
    banco = 'estruturadas'
    url = f'mysql+pymysql://{usuario}:{senha}@{host}:{porta}/{banco}'
    engine = create_engine(url)
    return engine