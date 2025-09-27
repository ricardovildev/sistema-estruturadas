import streamlit as st
import tempfile
from backend.conexao import engine
from importacao import (
    importar_notas_atualizado,
    importar_historico_precos,
    importar_proventos,
    importar_ativos,
    calcular_resultado_opcoes,
    importar_vencimentos_opcoes,
    atualizar_historico_operacoes,
    importar_ativos_yahoo,
    atualizar_asset_yahoo,
    importar_ativos_livres,
    
)


def render():
    st.info("Acesso administrativo liberado.")
    st.subheader("📥 Importações e Atualizações")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📄 Notas de Corretagem")
        arquivo_notas = st.file_uploader("Selecione o arquivo de notas", type=["csv", "xlsx"], key="notas")
        if arquivo_notas is not None:
            tipo = 'csv' if arquivo_notas.name.endswith('.csv') else 'excel'
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{tipo}") as tmp:
                tmp.write(arquivo_notas.getbuffer())
                caminho_temp = tmp.name
            if st.button("Importar Notas"):
                try:
                    importar_notas_atualizado(caminho_temp, tipo)
                    st.success("Notas de corretagem importadas com sucesso.")
                except Exception as e:
                    st.error(f"Erro ao importar notas: {e}")

        if st.button("Importar Histórico de Preços"):
            try:
                importar_historico_precos()
                st.success("Histórico de preços importado com sucesso.")
            except Exception as e:
                st.error(f"Erro ao importar histórico: {e}")

        if st.button("Importar Proventos"):
            try:
                importar_proventos()
                st.success("Proventos importados com sucesso.")
            except Exception as e:
                st.error(f"Erro ao importar proventos: {e}")

        if st.button("Importar Ativos"):
            try:
                importar_ativos()
                st.success("Ativos importados com sucesso.")
            except Exception as e:
                st.error(f"Erro ao importar ativos: {e}")

    with col2:
        if st.button("Calcular Resultado das Opções"):
            try:
                calcular_resultado_opcoes()
                st.success("Resultado das opções calculado com sucesso.")
            except Exception as e:
                st.error(f"Erro ao calcular resultado: {e}")

        if st.button("Importar Vencimento das Opções"):
            try:
                importar_vencimentos_opcoes()
                st.success("Vencimentos importados com sucesso.")
            except Exception as e:
                st.error(f"Erro ao importar vencimentos: {e}")

        if st.button("Atualizar Ativos com código .SA"):
            try:
                atualizar_asset_yahoo()
                st.success("Ativos atualizados com sucesso.")
            except Exception as e:
                st.error(f"Erro ao atualizar ativos: {e}")

    st.subheader("📈 Atualizar Histórico de Operações")
    if st.button("Atualizar histórico"):
        try:
            atualizar_historico_operacoes()
            st.success("✅ Histórico atualizado com sucesso!")
        except Exception as e:
            st.error(f"❌ Erro ao atualizar: {e}")

    st.subheader("📤 Importar Ativos do Yahoo Finance")
    arquivo_yahoo = st.file_uploader("Selecione o arquivo de ativos", type=['csv', 'xlsx'], key="yahoo")
    if arquivo_yahoo is not None:
        tipo = 'csv' if arquivo_yahoo.name.endswith('.csv') else 'excel'
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{tipo}") as tmp:
            tmp.write(arquivo_yahoo.getbuffer())
            caminho_temp = tmp.name
        if st.button("Importar ativos"):
            try:
                mensagem = importar_ativos_yahoo(caminho_temp, tipo)
                if "sucesso" in mensagem.lower():
                    st.success(mensagem)
                else:
                    st.error(mensagem)
            except Exception as e:
                st.error(f"Erro ao importar ativos do Yahoo: {e}")

    st.subheader("📤 Importar Ativos Livres")
    arquivo_livres = st.file_uploader("Selecione a planilha de ativos livres", type=["xlsx"], key="livres")
    if st.button("Importar dados"):
        if arquivo_livres is not None:
            importar_ativos_livres(arquivo_livres, engine)
        else:
            st.warning("Por favor, selecione um arquivo antes de importar.")
