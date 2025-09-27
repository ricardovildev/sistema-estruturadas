import streamlit as st
import pandas as pd
from importacao import engine, consolidar_notas_simples, obter_lista_assets, obter_preco_ultimo, atualizar_preco
from frontend.auth import require_usuario

def render():
    # 🔒 Verifica login e perfil
    require_usuario()
    usuario = st.session_state.usuario

    """
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
            st.warning(f"⚠️ Falha ao atualizar os seguintes ativos: {', '.join(falhas)}")"""

    # -----------------------------
    # DASH DE CONSULTA SIMPLIFICADA
    # -----------------------------
    st.title("Consulta de Prêmios Consolidados")
    st.markdown("Filtre por cliente, ativo e período para visualizar os prêmios líquidos e posições.")

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        cliente_input = st.text_input("Digite o nome do cliente:")

    with col2:
        ativos = pd.read_sql("SELECT DISTINCT ativo_base FROM notas", engine)
        ativo_selecionado = st.selectbox("Selecione o ativo (opcional):", ["Todos"] + ativos['ativo_base'].tolist())

    with col3:
        anos_disponiveis = pd.read_sql(
            "SELECT DISTINCT YEAR(data_registro) AS ano FROM notas ORDER BY ano DESC",
            engine
        )
        ano_selecionado = st.selectbox("Selecione o ano:", anos_disponiveis['ano'].tolist())

    with col4:
        data_inicio = pd.to_datetime(st.date_input("Data inicial"))

    with col5:
        data_fim = pd.to_datetime(st.date_input("Data final")) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    with col6:
        tipo_posicao = st.selectbox(
            "Tipo de posição:",
            ["Todas", "Ativas (quantidade > 0)", "Zeradas (quantidade <= 0)"]
        )

    if st.button("Filtrar Operações"):
        try:
            df_consulta = consolidar_notas_simples(data_inicio, data_fim, engine)

            # Filtros adicionais
            if cliente_input.strip():
                df_consulta = df_consulta[df_consulta['cliente'].str.contains(cliente_input.strip(), case=False)]

            if ativo_selecionado != "Todos":
                df_consulta = df_consulta[df_consulta['ativo_base'] == ativo_selecionado]

            if tipo_posicao == "Ativas (quantidade > 0)":
                df_consulta = df_consulta[df_consulta['quantidade_atual'] > 0]
            elif tipo_posicao == "Zeradas (quantidade <= 0)":
                df_consulta = df_consulta[df_consulta['quantidade_atual'] <= 0]

            if not df_consulta.empty:
                soma_premio = df_consulta['Premio_liquido'].sum()
                st.metric(label="Total de Prêmio Recebido", value=f"R$ {soma_premio:,.2f}")
            else:
                st.warning("Nenhum dado encontrado para calcular o Prêmio Líquido.")

            df_consulta = df_consulta.rename(columns={
                "conta": "Conta",
                "cliente": "Cliente",
                "ativo_base": "Ativo",
                "Premio_recebido": "Prêmio Recebido",
                "Premio_pago": "Prêmio Pago",
                "Premio_liquido": "Prêmio Líquido"
            })

            colunas_exibir = ["Conta", "Cliente", "Ativo", "Prêmio Recebido", "Prêmio Pago", "Prêmio Líquido"]
            df_consulta = df_consulta[colunas_exibir]

            st.dataframe(df_consulta, use_container_width=True)

            st.success(f"{len(df_consulta)} registros encontrados.")
        except Exception as e:
            st.error(f"Erro ao consolidar dados: {e}")