import pandas as pd

from src.util.util import (
    MA50,
    MA200,
    VOLUME,
)


def calculate_moving_averages(data: pd.DataFrame):
    data[MA50] = data["Close"].rolling(window=50).mean()
    data[MA200] = data["Close"].rolling(window=200).mean()
    data["MEAN_VOL"] = data[VOLUME].rolling(window=50).mean()

    return data


def calculate_percent_away_from_ma(data: pd.DataFrame):
    """
    2% -> 5% healthty
    5% -> 10% Still good
    15% -> extended
    """
    data["perct_from_ma50"] = (data["Close"] - data[MA50]) / data[MA50] * 100
    data["perct_from_ma200"] = (data["Close"] - data[MA200]) / data[MA200] * 100
    data["perct_from_mean_vol"] = (data[VOLUME] - data["MEAN_VOL"]) / data["MEAN_VOL"] * 100

    return data


def calculate_signals_for_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    data = calculate_moving_averages(data)
    data = calculate_percent_away_from_ma(data)

    return data
