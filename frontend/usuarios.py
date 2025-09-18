import pandas as pd
import numpy as np
import re
import sys
import os
import yfinance as yf
from datetime import datetime
from datetime import date
from sqlalchemy import text
from io import StringIO
import bcrypt
from backend.conexao import conectar

engine = conectar()  # cria a conexão uma vez

def usuario_existe(username):
    query = text("SELECT COUNT(*) FROM usuarios WHERE username = :username")
    with engine.connect() as conn:
        result = conn.execute(query, {"username": username}).scalar()
        return result > 0

def cadastrar_usuario(nome, username, senha, email, perfil="usuario"):
    if usuario_existe(username):
        print(f"⚠️ Usuário '{username}' já existe.")
        return

    senha_hash = bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()
    query = text("""
        INSERT INTO usuarios (nome, username, senha_hash, email, perfil)
        VALUES (:nome, :username, :senha_hash, :email, :perfil)
    """)
    try:
        with engine.begin() as conn:
            conn.execute(query, {
                "nome": nome,
                "username": username,
                "senha_hash": senha_hash,
                "email": email,
                "perfil": perfil
            })
        print("✅ Usuário criado com sucesso!")
    except Exception as e:
        print("❌ Erro ao criar usuário:", e)

def autenticar_usuario(username, senha_digitada):
    query = text("SELECT * FROM usuarios WHERE username = :username AND ativo = TRUE")
    with engine.connect() as conn:
        result = conn.execute(query, {"username": username}).mappings().fetchone()
        if result and bcrypt.checkpw(senha_digitada.encode(), result["senha_hash"].encode()):
            return result  # já é um dicionário
        return None