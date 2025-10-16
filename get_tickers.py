import requests
from bs4 import BeautifulSoup
import pandas as pd # <-- Adicionado o pandas

ARQUIVO_CSV_FILTRO = "altas_baixas_b3.csv" # Nome do arquivo CSV para o filtro

def get_tickers_from_infomoney():
    """Busca a lista de tickers na Infomoney."""
    url = "https://www.infomoney.com.br/cotacoes/empresas-b3/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        tickers = []

        linhas = soup.find_all("tr")
        for linha in linhas:
            # Pega todos os <td class="strong"> da linha
            tds = linha.find_all("td", class_="strong")
            for td in tds:
                ticker = td.get_text(strip=True)
                if ticker:
                    # NOTA: O sufixo ".SA" será adicionado APÓS o filtro para facilitar a comparação.
                    tickers.append(ticker) 

        # remove duplicatas
        tickers = list(set(tickers))
        return tickers

    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição HTTP: {e}")
        return []

# ----------------------------------------------------------------------
## Nova função para ler o CSV e retornar os tickers
# ----------------------------------------------------------------------

def get_tickers_from_csv(filename):
    """Lê o arquivo CSV e retorna uma lista de Tickers (sem o .SA)."""
    try:
        # Assumindo que a coluna de Ticker no CSV se chama 'Ticker'
        df = pd.read_csv(filename, sep=';', encoding='utf-8')
        
        # Filtra e limpa a coluna 'Ticker'
        if 'Acao' in df.columns:
            # Remove o sufixo ".SA" se ele existir no CSV, garantindo a comparação correta
            tickers_csv = df['Acao'].str.replace('.SA', '', regex=False).str.strip().tolist()
            return set(tickers_csv)
        else:
            print(f"⚠️ Aviso: Coluna 'Ticker' não encontrada em '{filename}'.")
            return set()
            
    except FileNotFoundError:
        print(f"⚠️ Erro: Arquivo de filtro '{filename}' não encontrado. Retornando lista vazia.")
        return set()
    except Exception as e:
        print(f"⚠️ Erro ao ler CSV de filtro: {e}")
        return set()

# ----------------------------------------------------------------------
## Lógica Principal (Inner Join/Interseção)
# ----------------------------------------------------------------------

# if __name__ == "__main__":
    
#     # 1. Obtém os tickers da web (sem o sufixo .SA)
#     print("Iniciando busca de tickers na Infomoney...")
#     tickers_web = set(get_tickers_from_infomoney())
    
#     # 2. Obtém os tickers do CSV (sem o sufixo .SA)
#     print(f"Lendo tickers de filtro do arquivo {ARQUIVO_CSV_FILTRO}...")
#     tickers_csv = get_tickers_from_csv(ARQUIVO_CSV_FILTRO)
    
#     # 3. Realiza a Interseção (Inner Join)
#     # Pega apenas os tickers que estão em AMBOS os conjuntos
#     tickers_filtrados = tickers_web.intersection(tickers_csv)
    
#     # 4. Adiciona o sufixo e ordena
#     lista_final_salvar = sorted([t + ".SA" for t in tickers_filtrados])

#     if lista_final_salvar:
#         with open("tickers_b3.txt", "w", encoding="utf-8") as f:
#             for t in lista_final_salvar:
#                 f.write(t + "\n")
        
#         print("\n✅ Processamento Concluído.")
#         print(f"   Tickers da Web: {len(tickers_web)}")
#         print(f"   Tickers do CSV de Filtro: {len(tickers_csv)}")
#         print(f"   Arquivo tickers_b3.txt salvo com {len(lista_final_salvar)} tickers (Interseção).")
#     else:
#         print("Nenhum ticker encontrado após a filtragem (interseção vazia).")