import os
import json
import time

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from datetime import datetime

import constants


def total_amount_news():
    tickers = os.listdir(constants.DATA_FOLDER)
    total_news = 0
    for ticker in tickers:
        df = pd.read_csv(constants.DATA_FOLDER + ticker)
        total_news += len(df.index)
    return total_news

def total_amount_price_points():
    tickers = os.listdir(constants.PRICE_FOLDER)
    total_prices = 0
    for ticker in tickers:
        df = pd.read_csv(constants.PRICE_FOLDER + ticker)
        total_prices += len(df.index)
    return total_prices


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


def get_news_market_impact(steps=[STEPS]):
    """
    :return: list of percentage changes
    """
    with open("index_data/index_data.json") as file:
        data = json.load(file)
    if not data:
        return

    counter = 0
    res_data = {x: [] for x in steps}
    for ticker, val in data.items():
        start_time = time.time()
        # news_df = get_news_df(ticker)
        price_df = get_price_df(ticker)
        prices = price_df["close"]
        for news_event in val:
            start_price = prices[news_event["startPriceIndex"]]
            end_price_index = news_event["endPriceIndex"]
            for step in steps:
                index = news_event["startPriceIndex"] + step
                if index > end_price_index:
                    continue
                end_price = prices[index]
                res_data[step].append(end_price / start_price)

        counter += 1
        print(f"{counter}/{len(data)}, processed {ticker} in {time.time() - start_time} seconds")

    return res_data


def get_price_changes(steps=[STEPS]):
    with open("index_data/index_data.json") as file:
        data = json.load(file)
    if not data:
        return

    counter = 0
    res_data = {x: [] for x in steps}
    for ticker, val in data.items():
        start_time = time.time()

        price_df = get_price_df(ticker)
        df_len = len(price_df.index)
        prices = np.array(price_df["close"])
        for i in range(df_len):
            start_price = prices[i]
            for step in steps:
                index = i + step
                if index >= df_len:
                    break
                end_price = prices[index]
                res_data[step].append(end_price / start_price)

        counter += 1
        print(f"{counter}/{len(data)}, processed {ticker} in {time.time() - start_time} seconds")

    return res_data


def find_best_steps():
    """
    Goes through specified steps and find variance in price and news data
    """
    steps = [i for i in range(50, 201, 10)]
    steps_res_folder = "steps_results/"

    price_res = get_price_changes(steps)
    news_res = get_news_market_impact(steps)

    for step in steps:
        price = np.array(price_res[step])
        news = np.array(news_res[step])

        try:
            price_var = np.var(price[~np.isnan(price)])
            news_var = np.var(news[~np.isnan(news)])

            res = {"price_var": price_var, "news_var": news_var}

            with open(steps_res_folder + str(step), 'w+') as file:
                json.dump(res, file, indent=4)

        except Exception as e:
            print(e)


def get_indexing():
    with open("index_data/index_data.json") as file:
        data = json.load(file)
    if not data:
        return
    return data


def create_training_data():
    steps = [1, 2, 3, 6]
    amount_of_news = total_amount_news()

    data = get_indexing()

    keys = ["datetime", "headline", "summary", "related", "lang", "source", "1stepChange", "2stepChange",
            "3stepChange", "6stepChange"]

    train_data_dict = {elem: [] for elem in keys}

    total_time = time.time()

    counter = 0
    processed_news_counter = 0
    for ticker, news in data.items():
        start_time = time.time()

        price_df = get_price_df(ticker)
        news_df = get_news_df(ticker)

        prices = np.array(price_df["close"])

        news_counter = 0
        for news_event in news:
            start_price = prices[news_event["startPriceIndex"]]
            end_price_index = news_event["endPriceIndex"]

            news_data = list(news_df.iloc[news_event["eventIndex"]])

            for step in steps:
                index = news_event["startPriceIndex"] + step
                if index > end_price_index:
                    news_data.append(-1)
                    continue

                end_price = prices[index]
                percent_change = (end_price / start_price)
                news_data.append(percent_change)

            for index, elem in enumerate(keys):
                train_data_dict[elem].append(news_data[index])

            news_counter += 1
        counter += 1
        processed_news_counter += news_counter
        time_left = (
                (((time.time() - total_time)) / processed_news_counter) * (amount_of_news - processed_news_counter))
        print(
            f"{counter}/{len(data)}, processed {news_counter} news from {ticker} in {(time.time() - start_time):.2f} seconds. ETR: {time_left / 60:.0f} minutes")

    train_data = pd.DataFrame.from_dict(train_data_dict)
    train_data.to_csv(f"train_data_{str(datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%s'))}")


if __name__ == "__main__":
    # get_price_before_news("AAT")
    create_training_data()
