import os

import pandas as pd

import constants


def add_stock_timezone(stock_df):
    stock_df["time"] = pd.to_datetime(stock_df["time"])
    stock_df["time"] = stock_df["time"].dt.tz_localize('EST')
    return stock_df


def get_tickers():
    """
    :returns list of all company tickers that we have price data for
    """
    return sorted(os.listdir(constants.PRICE_FOLDER))


def get_news_df(ticker):
    df = pd.read_csv(constants.DATA_FOLDER + ticker)
    df.datetime = pd.to_datetime(df.datetime)
    df = df.sort_values(by="datetime")
    df = df.reset_index(drop=True)
    return df


def get_price_df(ticker):
    df = pd.read_csv(constants.PRICE_FOLDER + ticker)
    df.time = pd.to_datetime(df.time)
    df = df.sort_values(by="time")
    df = df.reset_index(drop=True)
    return df


def add_timezone_all_stocks():
    tickers = get_tickers()
    counter = 0
    for ticker in tickers:
        counter += 1
        filepath = constants.PRICE_FOLDER + ticker
        df = pd.read_csv(filepath)
        df = add_stock_timezone(df)
        df.to_csv(filepath, index=None)
        print(f"{counter}/{len(tickers)}, processed: {filepath}")


def process_stock(ticker):
    """
    Finds index of price just before news was published
    :returns list of tuples (news index, price index)
    """
    news_df = get_news_df(ticker)
    price_df = get_price_df(ticker)

    stock_to_price_index = []
    index = 0
    for event_index in news_df.index:
        while index < len(price_df.index):
            # find closest in time
            if price_df.iloc[index].time > news_df.iloc[event_index].datetime:
                # TODO: point before was our price before news
                stock_to_price_index.append((event_index, index - 1))
                break

            index += 1
    return stock_to_price_index


def process_all_stocks():
    price_tickers = set(os.listdir(constants.PRICE_FOLDER))
    news_tickers = set(os.listdir(constants.DATA_FOLDER))

    tickers = price_tickers.intersection(news_tickers)

    for ticker in tickers:
        process_stock(ticker)

        break


if __name__ == "__main__":
    process_stock("AAT")
