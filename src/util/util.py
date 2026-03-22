from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR_LOCAL = PROJECT_ROOT / "data"
DATA_DIR = "data"
MACRO_DIR = "macro"
INDEX_DIR = "index"
EQUITY_DIR = "equities"

BRENT_YAHOO = "BZ"
MA50 = "MA50"
MA200 = "MA200"
VOLUME = "Volume"


# MACRO_TICKERS = {
#     "CPI": "CPIAUCSL",
#     "PPI": "PPIACO",
#     "UNEMPLOYMENT_RATE": "UNRATE",
#     "FED_FUNDS_RATE": "FEDFUNDS",
#     "GDP": "GDP",
#     "RETAIL_SALES": "RSAFS",
#     "INDUSTRIAL_PRODUCTION": "INDPRO",
#     "CONSUMER_SENTIMENT": "UMCSENT",
#     "HOUSING_STARTS": "HOUST",
#     "EXISTING_HOME_SALES": "EXHOSLUSM495S",
#     "NEW_HOME_SALES": "NHSLTOT",
#     "PERSONAL_INCOME": "PI",
#     "PERSONAL_SPENDING": "PCE"
# }

MACRO_TICKERS = {
    "SPY500": "SPY",
}

INDEX_TICKERS = {
    "TECHNOLOGY": "XLK",
    "HEALTHCARE": "XLV",
    "FINANCE": "XLF",
    "CONSUMER DISCRETIONARY": "XLY",
    "COMMUNICATION SERVICES": "XLC",
    "INDUSTRIAL": "XLI",
    "ENERGY": "XLE",
    "CONSUMER STAPLES": "XLP",
    "UTILITIES": "XLU",
    "REAL ESTATE": "XLRE",
    "MATERIALS": "XLB",
}

EQUITY_TICKERS = {
    "AAPL": "AAPL",
    "MSFT": "MSFT",
    "GOOGL": "GOOGL",
    "AMZN": "AMZN",
    "META": "META",
    "TSLA": "TSLA",
    "NVDA": "NVDA",
}

DATASETS = {
    "macro": (MACRO_DIR, MACRO_TICKERS),
    "index": (INDEX_DIR, INDEX_TICKERS),
    "equities": (EQUITY_DIR, EQUITY_TICKERS),
}
