import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import yfinance as yf


def download_ticker(file_path, processed_file_path):

    file_path = os.path.join("data", "raw", file_path)
    processed_file_path = os.path.join("data", "raw", processed_file_path)

    df = pd.read_csv(file_path, usecols=["Symbol", "Industry"])

    df["Symbol"] = df["Symbol"] + ".NS"

    df.to_csv(processed_file_path)
    print(df)


def download_ticker_data(ticker_file):
    Base_DIR = "data/processed"
    tikcer_file = os.path.join("data", "raw", ticker_file)

    df = pd.read_csv(tikcer_file)

    for symbol in df["Symbol"]:
        data = yf.download(
            symbol,
            start="2015-01-01",
            end="2025-01-01",
            interval="1d",
            auto_adjust=False,
        )

        if data.empty:
            print(f" No data for {symbol}")
            continue

        data.columns = data.columns.get_level_values(0)
        data.to_parquet(
            os.path.join(Base_DIR, f"{symbol}.parquet"),
            engine="pyarrow",
            index=True,
            compression="snappy",
        )
        print(f"{symbol} data is fetched")


# give file name as parameter in download_symbol
# -----------> (name of file to be read ,  name of file as to  write )


download_ticker("ind_nifty50list.csv", "nifty50_ticker.csv")


download_ticker_data("nifty50_ticker.csv")
