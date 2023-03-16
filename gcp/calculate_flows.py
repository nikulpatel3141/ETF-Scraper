"""Calculate flows 
"""

import logging
import os
from datetime import datetime, timedelta, date

import pandas as pd

FLOW_DELAY = 2  # cutoff calculations to 2 business days ago
LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", 21))
UNIVERSE_FUND = "IVV"

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_NAME = "etf_holdings"
HOLDINGS_TABLE_NAME = "etf_holdings"


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(filename)16s:%(lineno)4d:%(levelname)8s] - %(message)s",
)

logger = logging.getLogger(__name__)


DATASET_NAME = "etf_holdings"  # FIXME: repetition
HOLDINGS_TABLE_NAME = "etf_holdings"


HOLDINGS_TABLE = f"`{PROJECT_ID}.{DATASET_NAME}.{HOLDINGS_TABLE_NAME}`"
_FMT_TICKER = """REPLACE(REPLACE(ticker, " ", ""), ".", "")"""


def n_bdays_ago(date_: date, n: int) -> date:
    return (date_ - pd.offsets.BDay(n)).date()


def get_query_dates():
    """Convenience function for start + end dates for calculating flows"""
    cur_holdings_date = n_bdays_ago(datetime.today().date(), FLOW_DELAY)
    lookback_date = n_bdays_ago(cur_holdings_date, LOOKBACK_WINDOW)
    buffer_lookback_date = n_bdays_ago(lookback_date, FLOW_DELAY)

    return cur_holdings_date, lookback_date, buffer_lookback_date


def generate_flow_query(cur_holdings_date, lookback_date, buffer_lookback_date) -> str:
    return f"""
    WITH raw_holdings AS (
        SELECT fund_ticker, as_of_date, {_FMT_TICKER} ticker, amount, price, sector
        FROM {HOLDINGS_TABLE}
        WHERE as_of_date BETWEEN "{buffer_lookback_date}" AND "{cur_holdings_date}"
    ),
    subset_prices AS (
        SELECT as_of_date, ticker, AVG(price) price
        FROM raw_holdings
        WHERE
            fund_ticker = "{UNIVERSE_FUND}" AND
            sector NOT IN ('Cash and/or Derivatives')
        GROUP BY as_of_date, ticker
    ),
    grouped_holdings AS (
        SELECT fund_ticker, as_of_date, ticker, IFNULL(SUM(amount), 0) amount
        FROM raw_holdings
        WHERE ticker in (SELECT ticker FROM subset_prices)
        GROUP BY fund_ticker, as_of_date, ticker
    ),
    flows AS (
        SELECT as_of_date, ticker,
        CASE
            WHEN (amount IS NULL or LAG(amount, 1) OVER flow_window IS NULL) THEN NULL 
            ELSE amount - LAG(amount, 1) OVER flow_window
        END amount_diff
        FROM grouped_holdings
        WINDOW flow_window AS (PARTITION BY fund_ticker, ticker order by as_of_date)
    )
    SELECT flows.as_of_date, flows.ticker, SUM(amount_diff * price) flow
    FROM flows LEFT JOIN subset_prices
    ON
    flows.as_of_date = subset_prices.as_of_date AND
    flows.ticker = subset_prices.ticker 
    WHERE flows.as_of_date BETWEEN "{lookback_date}" AND "{cur_holdings_date}"
    GROUP BY flows.ticker
    HAVING flow != 0 and FLOW IS NOT NULL
    ORDER BY flow, flows.as_of_date, flows.ticker
    """


def main():
    # TODO: check sqlalchemy 2.* works
    cur_holdings_date, lookback_date, buffer_lookback_date = get_query_dates()
    flow_query = generate_flow_query(
        cur_holdings_date, lookback_date, buffer_lookback_date
    )

    df = pd.read_gbq(flow_query)
    # TODO: format results
