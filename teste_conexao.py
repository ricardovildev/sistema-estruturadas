from backend.conexao import conectar

try:
    engine = conectar()
    with engine.connect() as conn:
        result = conn.execute("SELECT 1")
        print("Conexão OK:", result.scalar())
except Exception as e:
    print("Erro:", e)