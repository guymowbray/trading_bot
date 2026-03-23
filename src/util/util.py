import re
import uuid
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR_LOCAL = PROJECT_ROOT / "data"
MARKET_DATA_DOMAIN = "market_data"
SIGNAL_DOMAIN = "signal"
MACRO_DIR = "macro"
INDEX_DIR = "index"
EQUITY_DIR = "equities"

SIGNALS_DIR = "signals"

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

DATASETS_SIGNALS = {
    "macro": (f"{MACRO_DIR} / {SIGNALS_DIR}", MACRO_TICKERS),
    "index": (f"{INDEX_DIR} / {SIGNALS_DIR}", INDEX_TICKERS),
    "equities": (f"{EQUITY_DIR} / {SIGNALS_DIR}", EQUITY_TICKERS),
}


def generate_execution_uuid() -> str:
    """
    Generates a unique execution UUID using the current timestamp and a random UUID.

    :return: A unique execution UUID. eg 20260315_120505_5f2e4c8b9a1d4e5f8c9b0a7d6e3f2a1
    20260315_120505 is the timestamp and 5f2e4c8b9a1d4e5f8c9b0a7d6e3f2a1 is the random UUID.
    """

    return f"{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}"


def parse_execution_id(execution_id):

    date_part, time_part, uuid = execution_id.split("_")

    formatted_date = datetime.strptime(date_part, "%Y%m%d").strftime("%Y/%m/%d")

    return {"date": formatted_date, "time": time_part, "uuid": uuid}


def create_and_validate_s3_filepath(data_domain, market_data_type, today_date, execution_uuid):
    """
    eg. signals/equities/2026/03/22/20260322_203402_dcb571f4f795490b83eb63e47815e52d

    Just add /{filename} after.
    """
    try:
        datetime.strptime(today_date, "%Y/%m/%d")
    except ValueError("Today date is not correct format."):
        return False

    file_path = f"{data_domain}/{market_data_type}/{today_date}/{execution_uuid}"

    pattern = r"^[a-zA-Z0-9_]+/[a-zA-Z0-9_]+/\d{4}/\d{2}/\d{2}/\d{8}_\d{6}_[a-f0-9]{32}$"

    if not re.match(pattern, file_path):
        raise ValueError("Invalid execution_id format")

    return file_path
