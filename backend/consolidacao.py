import pandas as pd
from backend.conexao import conectar

def consolidar_dados():
    engine = conectar()
    query = "SELECT * FROM sua_tabela"
    df = pd.read_sql(query, engine)
    return df