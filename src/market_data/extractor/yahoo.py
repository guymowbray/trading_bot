from datetime import datetime
import os

import numpy as np
import pandas as pd
import yfinance as yf
from src.util.util import EQUITY_DIR, EQUITY_TICKERS, MA50, MA200, INDEX_TICKERS, MACRO_DIR, INDEX_DIR, MACRO_TICKERS, DATA_DIR
import uuid


class TickerDataQuery:
    ticker: str
    period: str

class TickerDataResult:
    ticker: str
    data: pd.DataFrame

def get_ticker_data(ticker: str):
    "Gets ticker data using yahoo finance package."
    return yf.Ticker(ticker)


def get_last_row_data(data):
    return data.loc[data.index[-1]]

def get_timestamp_from_last_row(data) -> pd.Timestamp:
    last_row = get_last_row_data(data)
    return last_row.name

def get_row_where_date_equals(data, date):
    """
    Find row data where date equals.
    Date format is '2025-02-14'
    
    :param data: pandas dataframe
    :param date: date(str)
    """
    return data.loc[data.index == date + ' 00:00:00-05:00']

def check_current_price_above_MA200(data: pd.DataFrame) -> bool:
    """
    Checks to see if current price is above the MA200 value.
    
    :param data: Description
    :type data: pd.DataFrame
    :return: Description
    :rtype: bool
    """
    if data.iloc[0]["Close"] >= data.iloc[0][MA200]:
        return True
    else: 
        return False

def check_current_price_above_MA50(data: pd.DataFrame) -> bool:
    """
    Checks to see if current price is above the MA50 value. 
    
    :param data: Description
    :type data: pd.DataFrame
    :return: Description
    :rtype: bool
    """
    if data.iloc[0]["Close"] >= data.iloc[0][MA50]:
        return True
    else: 
        return False


def extract_ticker_data(ticker_list: dict = INDEX_TICKERS):
    """
    Extract index data.
    """
    payload = {}

    for _, ticker_symbol in ticker_list.items():
        data = get_ticker_data(ticker_symbol)
        data = data.history(period="1y")
        payload[ticker_symbol] = data
    
    return payload
    """
    Calculates the support levels based on the clusters of lows.

    Input: Python Dict of List of prices 
    """
    zones = []

    
    for cluster in clusters.values():
        
        lower = min(cluster)
        upper = max(cluster)
        mean = np.mean(cluster)

        zones.append({
            "support_mean": mean,
            "support_lower": lower,
            "support_upper": upper,
            "touches": len(cluster)
        })

    return zones

def save_pandas_dataframe(data: pd.DataFrame, filename: str, base_dir: str, file_extension: str):
    os.makedirs(base_dir, exist_ok=True)
    data.to_csv(f"{base_dir}/{filename}.{file_extension}")


def save_raw_data(payload: dict, base_dir: str = None, execution_uuid: str = "HELLO", file_extension: str = "parquet"):
    if base_dir is None:
        today_dir = datetime.today().strftime("%Y/%m/%d")
        base_dir = os.path.join(DATA_DIR, "raw", today_dir, "index")
    os.makedirs(base_dir, exist_ok=True)

    for ticker, data in payload.items():
        fname = f"{ticker}_{execution_uuid}"
        save_pandas_dataframe(data, fname, base_dir, file_extension)

    return True


def yahoo_main():
    today_date = datetime.today().strftime("%Y/%m/%d")
    run_uuid = uuid.uuid4().hex

    macro_dir = MACRO_DIR / today_date / run_uuid
    macro_data = extract_ticker_data(ticker_list=MACRO_TICKERS)
    save_raw_data(macro_data, macro_dir, run_uuid)

    index_dir = INDEX_DIR / today_date / run_uuid
    index_data = extract_ticker_data(ticker_list=INDEX_TICKERS)
    save_raw_data(index_data, index_dir, run_uuid)

    equities_dir = EQUITY_DIR / today_date / run_uuid
    equities_data = extract_ticker_data(ticker_list=EQUITY_TICKERS)
    save_raw_data(equities_data, equities_dir, run_uuid)

