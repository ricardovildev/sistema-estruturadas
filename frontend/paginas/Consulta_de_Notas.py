import streamlit as st
import pandas as pd
from frontend.auth import require_usuario
from backend.importacao import engine

def render():
    require_usuario()
    usuario = st.session_state.usuario

    st.title("📋 Consulta de Notas")
    st.markdown("Filtre por cliente, ativo, tipo de papel e período para visualizar as notas registradas.")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        cliente_input = st.text_input("Cliente (opcional):")

    with col2:
        ativo_input = st.text_input("Ativo Base (opcional):")

    with col3:
        data_inicio = st.date_input("Data Inicial")

    with col4:
        data_fim = st.date_input("Data Final")

    tipo_papel = st.selectbox("Tipo de Papel:", ["Todos", "AÇÃO", "OPCAO"])

    if st.button("🔍 Consultar Notas"):
        query = '''
        SELECT 
            data_registro, conta, cliente, q_negociacao, tipo_lado, tipo_mercado, ativo_base, quantidade, preco, valor_operacao,
            debito_credito, tipo_papel, tipo_opcao
        FROM notas
        WHERE 1=1
        '''
        params = []

        if cliente_input.strip():
            query += " AND cliente LIKE %s"
            params.append(f"%{cliente_input.strip()}%")

        if ativo_input.strip():
            query += " AND ativo_base LIKE %s"
            params.append(f"%{ativo_input.strip()}%")

        if data_inicio:
            query += " AND data_registro >= %s"
            params.append(data_inicio)

        if data_fim:
            query += " AND data_registro <= %s"
            params.append(data_fim)

        if tipo_papel != "Todos":
            query += " AND tipo_papel = %s"
            params.append(tipo_papel)

        query += " ORDER BY data_registro DESC"

        try:
            df_notas = pd.read_sql(query, engine, params=tuple(params))

            if df_notas.empty:
                st.warning("Nenhuma nota encontrada com os filtros selecionados.")
            else:
                colunas_moeda = ['preco','valor_operacao']
                for coluna in colunas_moeda:
                    df_notas[coluna] = pd.to_numeric(df_notas[coluna], errors='coerce')
                    df_notas[coluna] = df_notas[coluna].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "R$ 0,00")

                df_notas = df_notas.rename(columns={
                    "data_registro": "Data do Pregão",
                    "conta" : "Conta",
                    "cliente" : "Cliente",
                    "q_negociacao": "Negociação",
                    "tipo_lado": "C/V",
                    "tipo_mercado": "Tipo Mercado",
                    "ativo_base": "Ativo",
                    "quantidade": "Quantidade",
                    "preco": "Preço/Ajuste",
                    "valor_operacao": "Valor Operação/Ajuste",
                    "debito_credito": "D/C",
                    "tipo_papel": "Tipo Papel",
                    "tipo_opcao": "Tipo Opção"
                })

                st.dataframe(df_notas, use_container_width=True)
                st.success(f"{len(df_notas)} notas encontradas.")
        except Exception as e:
            st.error(f"Erro ao consultar notas: {e}")
