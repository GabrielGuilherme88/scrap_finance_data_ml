from s3_utils import handle_s3
from db_model import create_db_and_tables, get_db, table_exists, CotacaoAcao 
from b3_scrap_cotacao import rodar_pipeline_csv
from csv_ingestion import ler_csv_e_inserir_no_db, ler_csv_historico_e_inserir_no_db

# --- Nome da tabela baseado no ORM ---
TABLE_NAME = CotacaoAcao.__tablename__

# def truncate_table():
#     """Deleta todos os registros existentes na tabela de cotações."""
#     session = get_db()
#     try:
#         # Delete via query e commit explícito
#         num_deleted = session.query(CotacaoAcao).delete(synchronize_session=False)
#         session.commit()
#         print(f"TRUNCATE: {num_deleted} registros antigos deletados da tabela '{TABLE_NAME}'.")
#     except Exception as e:
#         session.rollback()
#         print(f"ERRO ao truncar a tabela: {e}")
#     finally:
#         session.close()

def rodar_pipeline_scraping():
    # 1. Configuração do DB
    if table_exists(TABLE_NAME): 
        print(f"AVISO: Tabela '{TABLE_NAME}' já existe. Não será recriada.")
    else:
        print(f"CRIANDO TABELA '{TABLE_NAME}'...")
        create_db_and_tables()

    # truncate_table()

    dados = rodar_pipeline_csv()
    
    if not dados or len(dados) == 0:
        print("\nPIPELINE Scrap encerrado: Dados baixados!")
        print("=" * 40)
        return   

if __name__ == "__main__":
    rodar_pipeline_scraping()
    ler_csv_e_inserir_no_db()
    ler_csv_historico_e_inserir_no_db()