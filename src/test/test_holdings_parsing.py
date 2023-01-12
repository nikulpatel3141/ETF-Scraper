"""
Tests we correctly parse responses for holdings data from different providers.

Note: these aren't comprehensive tests (so far), we're just testing things work as intended.
"""

from datetime import date
from pathlib import Path

import pytest
import numpy as np

from etf_scraper.scrapers import ISharesListings, SSGAListings

DATA_DIR = Path(__file__).parent.joinpath("test_data")


@pytest.fixture
def ishares_test_resp():
    resp_file = str(DATA_DIR.joinpath("IShares_IVV_holdings_20221230_resp.csv"))
    with open(resp_file, "r") as f:
        return f.read()


@pytest.fixture
def ssga_test_resp():
    resp_file = str(DATA_DIR.joinpath("SSGA_SPY_holdings_20230110_resp.xlsx"))
    with open(resp_file, "rb") as f:
        return f.read()


def check_holdings_df(
    holdings_df,
    exp_tot_holdings,
    exp_first_ticker,
    exp_last_ticker,
    exp_weight,
):
    """Perform some basic checks on the parsed responses for holdings dfs:
    - Check the total # of shares is as expected
    - Check the first and last tickers match
        - # BUG: doesn't work if the parser also sorts, but this doesn't happen
    - Check the sum of weights is as expected (should be close to 1, but will be off due
    to rounding errors)
    """
    act_tot_holdings = holdings_df["amount"].sum()
    assert act_tot_holdings == exp_tot_holdings  # integer so need exact match

    act_first_ticker = holdings_df["ticker"].iloc[0]
    assert act_first_ticker == exp_first_ticker

    act_last_ticker = holdings_df["ticker"].iloc[-1]
    assert act_last_ticker == exp_last_ticker

    assert np.isclose(
        holdings_df["weight"].sum(), exp_weight
    )  # floar can be slightly off


def test_ishares_etf_holdings_resp_parser(ishares_test_resp):
    # exp_ticker = "IVV"
    exp_holdings_date = date(year=2022, month=12, day=30)

    # calculated directly from the file
    exp_tot_holdings = 3690353839
    exp_weight = 100.02
    exp_first_ticker = "AAPL"
    exp_last_ticker = "ETD_USD"

    # with open(resp_file, "r") as f:
    holdings_df, act_holdings_date = ISharesListings._parse_holdings_resp(
        ishares_test_resp
    )

    assert act_holdings_date == exp_holdings_date

    check_holdings_df(
        holdings_df, exp_tot_holdings, exp_first_ticker, exp_last_ticker, exp_weight
    )


def test_ssga_etf_holdings_resp_parser(ssga_test_resp):

    exp_ticker = "SPY"
    exp_holdings_date = date(year=2023, month=1, day=10)

    # calculated directly from the file
    exp_tot_holdings = 3731709239
    exp_weight = 99.999959
    exp_first_ticker = "AAPL"
    exp_last_ticker = "NWS"

    holdings_df, act_holdings_date, act_ticker = SSGAListings._parse_holdings_resp(
        ssga_test_resp
    )
    print(holdings_df.dtypes)

    assert act_ticker == exp_ticker
    assert act_holdings_date == exp_holdings_date

    check_holdings_df(
        holdings_df, exp_tot_holdings, exp_first_ticker, exp_last_ticker, exp_weight
    )
