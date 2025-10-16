import requests
from bs4 import BeautifulSoup

def get_tickers_from_infomoney():
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
            # pega todos os <td class="strong"> da linha
            tds = linha.find_all("td", class_="strong")
            for td in tds:
                ticker = td.get_text(strip=True)
                if ticker:
                    tickers.append(ticker + ".SA")  # adiciona sufixo para yfinance

        # remove duplicatas e ordena
        tickers = sorted(list(set(tickers)))
        return tickers

    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição HTTP: {e}")
        return []

if __name__ == "__main__":
    lista_tickers = get_tickers_from_infomoney()
    if lista_tickers:
        with open("tickers_b3.txt", "w", encoding="utf-8") as f:
            for t in lista_tickers:
                f.write(t + "\n")
        print(f"Arquivo tickers_b3.txt salvo com {len(lista_tickers)} tickers.")
    else:
        print("Nenhum ticker encontrado.")