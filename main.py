from db_model import create_db_and_tables, table_exists, CotacaoAcao, HistoricoTicker
from b3_scrap_cotacao import rodar_pipeline_csv
from csv_ingestion import ler_csv_e_inserir_no_db, ler_csv_historico_e_inserir_no_db
from get_tickers import get_tickers_from_infomoney, get_tickers_from_csv, ARQUIVO_CSV_FILTRO
from get_data_yf import get_data_from_tickers, FILE_TXT, FILE_CSV
import os
import subprocess
import sys

##get_tickers deve ser executado depois do b3_scrap_cotaacao

# --- Nome da tabela baseado no ORM ---
TABLE_NAME = CotacaoAcao.__tablename__
TABLE_NAME = HistoricoTicker.__tablename__

def rodar_pipeline_scraping():
    # 1. Configuração do DB
    if table_exists(TABLE_NAME): 
        print(f"AVISO: Tabela '{TABLE_NAME}' já existe. Não será recriada.")
    else:
        print(f"CRIANDO TABELA '{TABLE_NAME}'...")
        create_db_and_tables()

    dados = rodar_pipeline_csv()
    
    if not dados or len(dados) == 0:
        print("\nPIPELINE Scrap encerrado: Dados baixados!")
        print("=" * 40)
        return   

if __name__ == "__main__":

    #Inicio do main para rodar o get_data_yf
    
    print("Iniciando busca de tickers na Infomoney...")
    tickers_web = set(get_tickers_from_infomoney())
    
    print(f"Lendo tickers de filtro do arquivo {ARQUIVO_CSV_FILTRO}...")
    tickers_csv = get_tickers_from_csv(ARQUIVO_CSV_FILTRO)
    
    tickers_filtrados = tickers_web.intersection(tickers_csv)
    
    lista_final_salvar = sorted([t + ".SA" for t in tickers_filtrados])

    if lista_final_salvar:
        with open("tickers_b3.txt", "w", encoding="utf-8") as f:
            for t in lista_final_salvar:
                f.write(t + "\n")
        
        print("\n✅ Processamento Concluído.")
        print(f"   Tickers da Web: {len(tickers_web)}")
        print(f"   Tickers do CSV de Filtro: {len(tickers_csv)}")
        print(f"   Arquivo tickers_b3.txt salvo com {len(lista_final_salvar)} tickers (Interseção).")
    else:
        print("Nenhum ticker encontrado após a filtragem (interseção vazia).")

    if not os.path.exists(FILE_TXT):
        subprocess.run([sys.executable, os.path.join(BASE_DIR, "get_tickers.py")], check=True)

    if not os.path.exists(FILE_TXT):
        print("Erro: não foi possível criar o arquivo tickers_b3.txt")
        sys.exit(1)
    
    #Inicio do main para rodar o get_tickers

    df = get_data_from_tickers(FILE_TXT, period="1mo", interval="1d")
    if not df.empty:
        # Salva em CSV
        df.to_csv(FILE_CSV, index=False, sep=";", encoding="utf-8")
        print(f"Arquivo historico_tickers.csv salvo com {len(df)} linhas.")
    else:
        print("Nenhum dado coletado.")

    rodar_pipeline_scraping()

    ler_csv_e_inserir_no_db()

    ler_csv_historico_e_inserir_no_db()