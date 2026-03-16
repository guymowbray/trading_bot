import pytest
import pandas as pd
from datetime import datetime


@pytest.fixture
def dummy_data():
    df = pd.DataFrame({"Close": [100], "MA200": [90], "MA50": [95]})
    df.index = [datetime(2026, 3, 15, 5, 0, 0)]
    return df
