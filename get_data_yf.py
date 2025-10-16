import yfinance as yf
import pandas as pd
import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_TXT = os.path.join(BASE_DIR, "tickers_b3.txt")
FILE_CSV = os.path.join(BASE_DIR, "historico_tickers.csv")  # arquivo CSV

def get_data_from_tickers(file_txt, period="1mo", interval="1d"):
    with open(file_txt, "r") as f:
        tickers = [linha.strip() for linha in f.readlines()]

    all_data = []

    for ticker in tickers:
        try:
            print(f"Baixando dados de {ticker} ...")
            tk = yf.Ticker(ticker)
            hist = tk.history(period=period, interval=interval)

            if not hist.empty:
                hist = hist.reset_index()
                hist["Ticker"] = ticker

                if "Date" in hist.columns:
                    # Converte para UTC e remove timezone
                    hist["Date"] = pd.to_datetime(hist["Date"], utc=True)
                    hist["Date"] = hist["Date"].dt.tz_convert("UTC")
                    hist["Date"] = hist["Date"].dt.tz_localize(None)  # remove tz info

                all_data.append(hist)
        except Exception as e:
            print(f"Erro ao coletar {ticker}: {e}")

    if all_data:
        df_final = pd.concat(all_data, ignore_index=True)
        return df_final
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    if not os.path.exists(FILE_TXT):
        subprocess.run([sys.executable, os.path.join(BASE_DIR, "get_tickers.py")], check=True)

    if not os.path.exists(FILE_TXT):
        print("Erro: não foi possível criar o arquivo tickers_b3.txt")
        sys.exit(1)

    df = get_data_from_tickers(FILE_TXT, period="1mo", interval="1d")
    if not df.empty:
        # Salva em CSV
        df.to_csv(FILE_CSV, index=False, sep=";", encoding="utf-8")
        print(f"Arquivo historico_tickers.csv salvo com {len(df)} linhas.")
    else:
        print("Nenhum dado coletado.")