import streamlit as st
import pandas as pd
from importacao import (
    obter_lista_assets,
    obter_preco_ultimo,
    atualizar_preco,
    engine
)
from frontend.auth import require_usuario

def render():
    # 🔒 Verifica login e perfil
    require_usuario()
    usuario = st.session_state.usuario

    # -----------------------------
    # ATUALIZAÇÃO DE PREÇOS
    # -----------------------------
    st.title("🎯 Consulta de Prêmios")
    st.write("Aqui você pode atualizar os preços dos ativos cadastrados.")

    if st.button("🔄 Atualizar todos os preços"):
        df_assets = obter_lista_assets(engine)
        atualizados = 0
        falhas = []

        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, row in df_assets.iterrows():
            ticker_yahoo = row['asset_original'].strip().upper() + ".SA"
            status_text.text(f"🔍 Atualizando {row['asset_original']}...")
            preco = obter_preco_ultimo(ticker_yahoo)

            if preco is not None:
                atualizar_preco(engine, row['asset_original'], preco)
                atualizados += 1
            else:
                falhas.append(row['asset_original'])

            progress_bar.progress((i + 1) / len(df_assets))

        st.success(f"✅ {atualizados} ativos atualizados com sucesso.")
        if falhas:
            st.warning(f"⚠️ Falha ao atualizar os seguintes ativos: {', '.join(falhas)}")

    # -----------------------------
    # DASH DE CONSULTA
    # -----------------------------
    st.title("Consulta de Operações Consolidadas")
    st.markdown("Filtre por cliente, ativo e período para visualizar o histórico de operações.")

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        cliente_input = st.text_input("Digite o nome do cliente:")

    with col2:
        try:
            ativos = pd.read_sql("SELECT DISTINCT ativo_base FROM historico_operacoes", engine)
            ativo_selecionado = st.selectbox("Selecione o ativo (opcional):", ["Todos"] + ativos['ativo_base'].tolist())
        except Exception:
            ativo_selecionado = "Todos"

        anos_disponiveis = pd.read_sql(
            "SELECT DISTINCT YEAR(data_inicio) AS ano FROM historico_operacoes ORDER BY ano DESC",
            engine
        )

    with col3:
        ano_selecionado = st.selectbox("Selecione o ano:", anos_disponiveis['ano'].tolist())

    with col4:
        data_inicio = st.date_input("Data inicial")

    with col5:
        data_fim = st.date_input("Data final")

    with col6:
        tipo_posicao = st.selectbox(
            "Tipo de posição:",
            ["Todas", "Ativas (quantidade > 0)", "Zeradas (quantidade <= 0)"]
        )

    if st.button("Filtrar Operações"):
        query = """
        SELECT 
            data_inicio, conta, cliente, ativo_base, quantidade_atual, preco_medio, 
            preco_fechamento, investido, Posicao_atual, resultado_sem_opcoes,Premio_liquido, resultado_com_opcoes
        FROM historico_operacoes
        WHERE 1=1
        """
        params = []

        if cliente_input.strip() != "":
            query += " AND cliente LIKE %s"
            params.append(f"%{cliente_input.strip()}%")

        if ativo_selecionado != "Todos":
            query += " AND ativo_base = %s"
            params.append(ativo_selecionado)

        if data_inicio:
            query += " AND data_inicio >= %s"
            params.append(data_inicio)
        if data_fim:
            query += " AND data_inicio <= %s"
            params.append(data_fim)

        if tipo_posicao == "Ativas (quantidade > 0)":
            query += " AND quantidade_atual > 0"
        elif tipo_posicao == "Zeradas (quantidade <= 0)":
            query += " AND quantidade_atual <= 0"

        query += " ORDER BY cliente, ativo_base"

        try:
            df_consulta = pd.read_sql(query, engine, params=tuple(params))

            if not df_consulta.empty:
                soma_premio = df_consulta['Premio_liquido'].sum()
                st.metric(label="Total de Prêmio Recebido", value=f"R$ {soma_premio:,.2f}")
            else:
                st.warning("Nenhum dado encontrado para calcular o Prêmio Líquido.")

            colunas_moeda = [
                'preco_medio', 'preco_fechamento', 'investido', 'Posicao_atual',
                'resultado_sem_opcoes', 'Premio_liquido', 'resultado_com_opcoes'
            ]

            for coluna in colunas_moeda:
                if coluna in df_consulta.columns:
                    df_consulta[coluna] = df_consulta[coluna].apply(
                        lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "R$ 0,00"
                    )

            df_consulta = df_consulta.rename(columns={
                "data_inicio": "Data de Inicio",
                "conta": "Conta",
                "cliente": "Cliente",
                "ativo_base": "Ativo",
                "quantidade_atual": "Quantidade Atual",
                "preco_medio": "Preço Médio",
                "preco_fechamento": "Preço Atual",
                "investido": "Valor Investido",
                "Posicao_atual": "Posição Atual",
                "resultado_sem_opcoes": "Resultado sem Opções",
                "Premio_liquido": "Prêmios Recebidos",
                "resultado_com_opcoes": "Resultado com Opções"
            })

            st.dataframe(df_consulta, use_container_width=True)
            st.success(f"{len(df_consulta)} registros encontrados.")

            # -----------------------------
            # DASH DE RESUMO MENSAL
            # -----------------------------
            st.title("Prêmio Mensal")

            query_resumo = """
            SELECT 
                cliente,
                MONTH(data_registro) AS mes,
                SUM(
                    CASE 
                        WHEN tipo_lado = 'V' THEN valor_operacao
                        WHEN tipo_lado = 'C' THEN -valor_operacao
                        ELSE 0
                    END
                ) AS premio_total
            FROM notas
            WHERE 
                YEAR(data_registro) = %s
                AND tipo_papel = 'OPCAO'
            """
            params = [ano_selecionado]

            if cliente_input.strip() != "":
                query_resumo += " AND cliente LIKE %s"
                params.append(f"%{cliente_input.strip()}%")

            query_resumo += " GROUP BY cliente, mes ORDER BY cliente, mes"

            df_resumo = pd.read_sql(query_resumo, engine, params=tuple(params))

            mes_abreviado = {
                1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr',
                5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Ago',
                9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
            }
            df_resumo['mes'] = df_resumo['mes'].apply(lambda m: f"{mes_abreviado[m]}-{ano_selecionado}")
            ordem_meses = [f"{mes_abreviado[m]}-{ano_selecionado}" for m in range(1, 13)]
            df_resumo['mes'] = pd.Categorical(df_resumo['mes'], categories=ordem_meses, ordered=True)

            df_pivot = df_resumo.pivot(index='cliente', columns='mes', values='premio_total').fillna(0)
            df_pivot = df_pivot.sort_index()
            df_pivot_formatado = df_pivot.applymap(lambda x: f"R$ {x:,.2f}")

            st.subheader(f"Prêmio Recebido - Ano {ano_selecionado}")
            st.dataframe(df_pivot_formatado)

        except Exception as e:
            st.error(f"❌ Erro ao consultar: {e}")