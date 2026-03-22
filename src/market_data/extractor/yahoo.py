import yfinance as yf


def get_ticker_data(ticker: str):
    "Gets ticker data using yahoo finance package."
    return yf.Ticker(ticker)
