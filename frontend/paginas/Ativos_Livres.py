import streamlit as st
import pandas as pd
from backend.importacao import (
    importar_ativos_livres,
    atualizar_preco_atual_ativos_livres,
    obter_lista_assets,
    obter_preco_ultimo,
    atualizar_preco,
    engine
)

def render():
    # 🔒 Verifica se o usuário está logado e tem perfil permitido
    if "usuario" not in st.session_state or st.session_state.usuario is None:
        st.warning("🔒 Você precisa estar logado.")
        st.stop()

    usuario = st.session_state.usuario
    if usuario["perfil"] not in ["admin", "usuario"]:
        st.warning("🔒 Acesso restrito.")
        st.stop()

    st.set_page_config(page_title="Dashboard de Ativos", layout="wide")
    st.title("📊 Dashboard de Ativos Livres")

    st.write("Aqui você pode atualizar os preços dos ativos cadastrados.")


    if st.button("🔄 Buscar preços yahoo"):
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

           
   

    # Conectar e carregar dados
    
    df = pd.read_sql("SELECT * FROM ativos_livres", con=engine)

    if df.empty:
        st.warning("Nenhum dado encontrado.")
        return

    # Agrupamento de filtros
    st.markdown("### 🔍 Filtros de Identificação")
    col1, col2, col3, col4 = st.columns(4)
    cliente_busca = col1.text_input("Buscar Cliente")
    ativo_sel = col2.selectbox("Ativo", ["Todos"] + sorted(df['Ativo'].dropna().unique()))
    assessor_sel = col3.text_input("Buscar por Assessor")
    mesa_sel = col4.selectbox("Mesa", ["Todos"] + sorted(df['Mesa'].dropna().unique()))

    st.markdown("---")

    st.markdown("### 📦 Filtros Numéricos")
    col5, col6 = st.columns(2)
    qtde_minima = col5.number_input("Qtde Livre mínima", min_value=0, value=0)
    volume_minimo = col6.number_input("Volume Livre mínima", min_value=0.0, value=0.0)



    # Botão para aplicar filtro
    if st.button("Aplicar filtro"):

        # Aplicar filtros
        df_filtrado = df.copy()
        if cliente_busca:
            df_filtrado = df_filtrado[df_filtrado['Cliente'].str.contains(cliente_busca, case=False, na=False)]
        if ativo_sel != "Todos":
            df_filtrado = df_filtrado[df_filtrado['Ativo'] == ativo_sel]
        if assessor_sel != "Todos":
            df_filtrado = df_filtrado[df_filtrado['Assessor'].str.contains(assessor_sel, case=False, na=False)]
        if mesa_sel != "Todos":
            df_filtrado = df_filtrado[df_filtrado['Mesa'] == mesa_sel]
        df_filtrado = df_filtrado[df_filtrado['Qtde_Livre'].fillna(0) > qtde_minima]
        df_filtrado = df_filtrado[df_filtrado['Volume_Livre'].fillna(0) > volume_minimo]

        # Calcular soma do Volume Livre filtrado
        volume_total = df_filtrado['Volume_Livre'].sum(skipna=True)

        # Exibir como métrica no topo
        st.metric(label="💰 Volume Livre Total (filtrado)", value=f"R$ {volume_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        df_filtrado = df_filtrado.sort_values(by='Volume_Livre', ascending=False, na_position='last')

        # Formatação
        def format_brl(x):
            return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        def format_pct(x):
            return f"{x:.2f} %".replace(".", ",")

        df_formatado = df_filtrado.copy()
        df_formatado['Preco_Medio'] = df_formatado['Preco_Medio'].apply(format_brl)
        df_formatado['Preco_Atual'] = df_formatado['Preco_Atual'].apply(format_brl)
        df_formatado['Volume_Livre'] = df_formatado['Volume_Livre'].apply(format_brl)
        df_formatado['Rentabilidade'] = df_formatado['Rentabilidade'].apply(format_pct)

        # Seleção de colunas e renomeação
        colunas_exibir = ['Conta', 'Cliente', 'Ativo', 'Assessor', 'Qtde_Total', 'Qtde_Livre',
                          'Preco_Medio', 'Preco_Atual', 'Volume_Livre', 'Rentabilidade']

        nomes_personalizados = {
            'Conta': 'Conta',
            'Cliente': 'Cliente',
            'Ativo': 'Ativo',
            'Assessor': 'Assessor',
            'Qtde_Total': 'Quantidade Total',
            'Qtde_Livre': 'Quantidade Livre',
            'Preco_Medio': 'Preço Médio',
            'Preco_Atual': 'Preço Atual',
            'Volume_Livre': 'Volume Livre',
            'Rentabilidade': 'Rentabilidade'
        }

        df_final = df_formatado[colunas_exibir].rename(columns=nomes_personalizados)

        st.markdown("---")
        st.subheader("📋 Tabela de Ativos Formatada")
        st.dataframe(df_final, use_container_width=True)

        st.caption(f"🔎 {len(df_final)} ativos encontrados com os filtros aplicados.")
        
    if st.button("Atualizar Preço Atual da Tabela"):
        atualizar_preco_atual_ativos_livres()    
