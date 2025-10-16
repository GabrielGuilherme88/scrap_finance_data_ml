import pandas as pd
from data_insert import salvar_dados_no_db, salvar_historico_no_db

# Caminhos dos arquivos CSV
ARQUIVO_COTACOES = "altas_baixas_b3.csv"
ARQUIVO_HISTORICO = "historico_tickers.csv"

# =======================================================
# Funções auxiliares
# =======================================================

def _converter_valor_para_float(valor):
    """Converte strings tipo '1,23%' ou '-' em float."""
    if pd.isna(valor) or valor in ('-', ''):
        return None
    try:
        if isinstance(valor, str):
            valor_limpo = valor.replace('%', '').replace(',', '.').replace('+', '').strip()
            return float(valor_limpo)
        return float(valor)
    except ValueError:
        return None


def _converter_data(valor):
    """Tenta converter uma string de data para datetime."""
    try:
        return pd.to_datetime(valor, errors='coerce')
    except Exception:
        return None


# =======================================================
# Ingestão de Cotações Atuais (já existente)
# =======================================================

def ler_csv_e_inserir_no_db():
    """Lê altas/baixas e insere em cotacoes_acoes."""
    print(f"\n--- INGESTÃO DE CSV DE COTAÇÕES ---")

    try:
        df = pd.read_csv(ARQUIVO_COTACOES, sep=';', decimal=',')
    except FileNotFoundError:
        print(f"ERRO: Arquivo '{ARQUIVO_COTACOES}' não encontrado.")
        return
    except Exception as e:
        print(f"ERRO ao ler CSV de cotações: {e}")
        return

    # Conversão de colunas numéricas
    float_cols = ['Ultima', 'VarDia', 'VarSemana', 'VarMes', 'VarAno', 'MinDia', 'MaxDia']
    for col in float_cols:
        if col in df.columns:
            df[col] = df[col].apply(_converter_valor_para_float)

    # Converte em lista de dicionários
    dados_para_db = df.to_dict('records')

    # Chama a função existente
    salvar_dados_no_db(dados_para_db)
    print(f"--- FIM DA INGESTÃO DE COTAÇÕES ---")


# =======================================================
# Ingestão de Histórico (nova parte)
# =======================================================

def ler_csv_historico_e_inserir_no_db():
    """Lê o CSV historico_tickers.csv e grava na tabela HistoricoTicker."""
    print(f"\n--- INGESTÃO DE CSV HISTÓRICO ---")

    try:
        df = pd.read_csv(ARQUIVO_HISTORICO, sep=';', decimal=',')
    except FileNotFoundError:
        print(f"ERRO: Arquivo '{ARQUIVO_HISTORICO}' não encontrado.")
        return
    except Exception as e:
        print(f"ERRO ao ler CSV histórico: {e}")
        return

    # ✅ Renomeia colunas do CSV para combinar com o modelo ORM
    df.rename(columns={
        "Stock Splits": "Stock_Splits",
        "Capital Gains": "Capital_Gains"
    }, inplace=True)

    # Converte colunas
    df["Date"] = df["Date"].apply(_converter_data)
    float_cols = ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock_Splits", "Capital_Gains"]
    for col in float_cols:
        if col in df.columns:
            df[col] = df[col].apply(_converter_valor_para_float)

    # Converte para lista de dicionários
    dados_para_db = df.to_dict("records")

    # Insere/atualiza no DB
    salvar_historico_no_db(dados_para_db)
    print(f"--- FIM DA INGESTÃO DE HISTÓRICO ---")


# =======================================================
# Execução manual (teste)
# =======================================================
# if __name__ == '__main__':
#     create_db_and_tables()
#     ler_csv_e_inserir_no_db()
#     ler_csv_historico_e_inserir_no_db()
