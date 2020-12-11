import os
import json
import time

import pandas as pd
import matplotlib.pyplot as plt

import constants


def total_amount_news():
    tickers = os.listdir(constants.DATA_FOLDER)
    total_news = 0
    for ticker in tickers:
        df = pd.read_csv(constants.DATA_FOLDER + ticker)
        total_news += len(df.index)
    return total_news


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


def get_price_before_news(ticker, steps=1):
    """
    Finds index of price just before news was published
    :returns list of maps (news index, price before index, price after index)
    """
    news_df = get_news_df(ticker)
    price_df = get_price_df(ticker)

    stock_to_price_index = []
    index = 0
    for event_index in news_df.index:
        while index < len(price_df.index):
            # find closest in time
            if price_df.iloc[index].time > news_df.iloc[event_index].datetime:
                start_price_index = index - 1
                future_price_index = start_price_index + steps
                if start_price_index <= 0:
                    break

                # Max index <= steps which there are prices
                end_price_max_index = min(future_price_index, len(price_df.index) - 1)

                stock_to_price_index.append(
                    {"eventIndex": event_index,
                     "startPriceIndex": start_price_index,
                     "endPriceIndex": end_price_max_index})
                break

            index += 1
    return stock_to_price_index


def get_price_after_news(ticker, news_to_price_index, steps):
    """

    :param news_to_price_index: List of tuples (news index, price index)
    :param steps: Amount of 30min steps from price to take
    :return: list of stock index to price index after
    """

    pass


def process_all_stocks():
    price_tickers = set(os.listdir(constants.PRICE_FOLDER))
    news_tickers = set(os.listdir(constants.DATA_FOLDER))

    tickers = sorted(price_tickers.intersection(news_tickers))
    steps = 100
    data = {}
    counter = 0
    for ticker in tickers:
        start_time = time.time()
        counter += 1
        news_to_price_index = get_price_before_news(ticker, steps)
        data[ticker] = news_to_price_index
        print(f"{counter}/{len(tickers)}, processed {ticker} in {time.time() - start_time} seconds")
    with open("index_data/index_data.json", 'w') as file:
        json.dump(data, file, sort_keys=True, indent=4)
    return data


STEPS = 1


def get_news_market_impact():
    """
    :return: list of percentage changes
    """
    with open("index_data/index_data.json") as file:
        data = json.load(file)
    if not data:
        return

    counter = 0
    res_data = []
    for ticker, val in data.items():
        start_time = time.time()
        news_df = get_news_df(ticker)
        price_df = get_price_df(ticker)
        for news_event in val:
            start_price = price_df.iloc[news_event["startPriceIndex"]].close
            end_price_index = news_event["endPriceIndex"]
            index = news_event["startPriceIndex"] + STEPS
            if index > end_price_index:
                continue
            end_price = price_df.iloc[index].close
            res_data.append(end_price / start_price)
        counter += 1
        print(f"{counter}/{len(data)}, processed {ticker} in {time.time() - start_time} seconds")

    return res_data


def get_price_changes():
    with open("index_data/index_data.json") as file:
        data = json.load(file)
    if not data:
        return

    counter = 0
    res_data = []
    for ticker, val in data.items():
        start_time = time.time()

        price_df = get_price_df(ticker)
        for i in range(len(price_df.index) - STEPS):
            start_price = price_df.iloc[i].close
            end_price = price_df.iloc[i+STEPS].close
            res_data.append(end_price / start_price)

        counter += 1
        print(f"{counter}/{len(data)}, processed {ticker} in {time.time() - start_time} seconds")

        if len(res_data) > 500000:
            break

    return res_data


if __name__ == "__main__":
    get_price_before_news("AAT")
