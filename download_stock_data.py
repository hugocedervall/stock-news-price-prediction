import pyEX as p
import pandas as pd
import os

UPDATE_FILES = False  # Get ticker if already downloaded?
SANDBOX = False
DATA_FOLDER = "news_data_test/" if SANDBOX else "news_data/"
PRICE_FOLDER = "price_data/"
TICKER_FILE = "nyse_tickers.txt"

api_token = "Tsk_bb7989a0d56a400ebcdcfcd1827c7f8e" if SANDBOX else "sk_d3676fd812c84568b062d0176ad61667"
version = "sandbox" if SANDBOX else "v1"


def get_tickers():
    """
    Returns: list of all company tickers in file
    """
    tickers = []
    with open(TICKER_FILE) as file:
        for line in file:
            tickers.append(line.strip())
    return tickers


c = p.Client(version=version, api_token=api_token)


#p.stocks.newsDF()
tickers = get_tickers()[:100]

def get_news():
    # Vill ha related==ticker, source=valdsource, lang=en
    filter="datetime,headline,summary,related,lang,source"
    #filter="?lang=en,summary,lang"
    counter = 0
    for ticker in tickers:

        if not UPDATE_FILES:
            if ticker in os.listdir(DATA_FOLDER):
                continue

        try:
            newsDf = c.newsDF(symbol=ticker, count=-1, filter=filter)
        except Exception as e:
            print(f"could not download news for {ticker} : {e}")
            continue
        newsDf.to_csv(DATA_FOLDER + ticker)
        counter += 1
        print(f"{counter}/{len(tickers)}: Downloaded {len(newsDf.index)} news for {ticker}")


def get_prices():
    for ticker in tickers:
        try:
            p.stocks.priceDF()
            priceDF = c.priceDF(symbol=ticker,version=version, )
        except Exception as e:
            print(f"could not download prices for {ticker} : {e}")
            continue
        priceDF.to_csv(DATA_FOLDER + ticker)
        print(f"Downloaded {len(priceDF.index)} news for {ticker}")
