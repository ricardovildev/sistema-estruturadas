import sys
import os
from frontend.usuarios import cadastrar_usuario


cadastrar_usuario(
    nome="Jose Ricardo",
    username="josericardo",
    senha="EjDmA0309@",
    email="ricardoviljunior@gmail.com",
    perfil="admin"
)
print("✅ Usuário admin criado com sucesso!")