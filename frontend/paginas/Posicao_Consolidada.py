import streamlit as st
import pandas as pd
from backend.importacao import engine, obter_lista_assets, obter_preco_ultimo, atualizar_preco
from frontend.auth import require_usuario


def render():
    # 🔒 Verifica login e perfil
    require_usuario()
    usuario = st.session_state.usuario


    # -----------------------------
    # ATUALIZAÇÃO DE PREÇOS
    # -----------------------------
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
            "SELECT DISTINCT YEAR(data_registro) AS ano FROM notas ORDER BY ano DESC",
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
            ["Ativas (quantidade > 0)", "Zeradas (quantidade <= 0)"]
        )


    if st.button("Filtrar Operações"):
        query = """
        SELECT 
            conta, cliente, ativo_base,quantidade_atual, preco_medio, preco_medio_vendas, 
            preco_fechamento, resultado_sem_opcoes, Premio_liquido, resultado_com_opcoes, 
            rentabilidade_venda_sem_premio, rentabilidade_venda_com_premio
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


            if tipo_posicao == "Zeradas (quantidade <= 0)":
                cols_exibir = [
                    'conta', 'cliente', 'ativo_base','quantidade_atual', 'preco_medio', 'preco_medio_vendas', 
                    'rentabilidade_venda_sem_premio', 'Premio_liquido', 'rentabilidade_venda_com_premio'
                ]
            else:
                cols_exibir = [
                    'conta', 'cliente','ativo_base', 'quantidade_atual', 'preco_medio', 'preco_fechamento',
                    'resultado_sem_opcoes', 'Premio_liquido', 'resultado_com_opcoes'
                ]


            # Formatar colunas monetárias e valores
            colunas_formatar = [
                'preco_medio', 'preco_medio_vendas', 'preco_fechamento', 'resultado_sem_opcoes', 'resultado_com_opcoes',
                'rentabilidade_venda_sem_premio', 'rentabilidade_venda_com_premio', 'Premio_liquido'
            ]

            for col in colunas_formatar:
                if col in df_consulta.columns:
                    df_consulta[col] = df_consulta[col].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "R$ 0,00")

            df_consulta = df_consulta.rename(columns={
                "conta": "Conta",
                "cliente": "Cliente",
                "ativo_base": "Ativo",
                "quantidade_atual": "Quantidade Atual",
                "preco_medio": "Preço Médio",
                "preco_medio_vendas": "Preço Médio de Vendas",
                "rentabilidade_venda_sem_premio": "Rentabilidade Venda Sem Prêmio",
                "rentabilidade_venda_com_premio": "Rentabilidade Venda Com Prêmio",                
                "Premio_recebido": "Prêmio Recebido",
                "Premio_pago": "Prêmio Pago",
                "Premio_liquido": "Prêmio Líquido"
            })


            st.dataframe(df_consulta[cols_exibir], use_container_width=True)
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
            params_resumo = [ano_selecionado]


            if cliente_input.strip() != "":
                query_resumo += " AND cliente LIKE %s"
                params_resumo.append(f"%{cliente_input.strip()}%")


            query_resumo += " GROUP BY cliente, mes ORDER BY cliente, mes"


            df_resumo = pd.read_sql(query_resumo, engine, params=tuple(params_resumo))


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
