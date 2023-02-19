from datetime import datetime, timedelta, date

import pandas as pd

LOOKBACK_WINDOW = 21
UNIVERSE_FUND = "IVV"


def n_bdays_ago(date_: date, n: int) -> date:
    return (date_ - pd.offsets.BDay(n)).date()


cur_holdings_date = n_bdays_ago(datetime.today().date(), 2)
lookback_date = n_bdays_ago(cur_holdings_date, LOOKBACK_WINDOW)

buffer_lookback_date_ = n_bdays_ago(lookback_date, 2)

holdings_table = "`{PROJECT_ID}.etf_holdings.etf_holdings`"
fmt_ticker = """REPLACE(REPLACE(ticker, " ", ""), ".", "")"""


holding_query = f"""
WITH raw_holdings AS (
    SELECT fund_ticker, as_of_date, {fmt_ticker} ticker, amount, price
    FROM {holdings_table}
    WHERE as_of_date BETWEEN "{buffer_lookback_date_}" AND "{cur_holdings_date}"
),
subset_prices AS (
    SELECT as_of_date, ticker, AVG(price) price
    FROM raw_holdings
    WHERE fund_ticker = "{UNIVERSE_FUND}"
    GROUP BY as_of_date, ticker
),
grouped_holdings AS (
    SELECT fund_ticker, as_of_date, ticker, IFNULL(SUM(amount), 0) amount
    FROM raw_holdings
    WHERE ticker in (SELECT ticker FROM subset_prices)
    GROUP BY fund_ticker, as_of_date, ticker
),
flows AS (
    SELECT as_of_date, ticker, amount - LAG(amount, 1) OVER flow_window amount_diff
    FROM grouped_holdings
    WINDOW flow_window AS (PARTITION BY fund_ticker, ticker order by as_of_date)
)
SELECT as_of_date, ticker, SUM(amount_diff) flow
FROM flows
WHERE as_of_date BETWEEN "{lookback_date}" AND "{cur_holdings_date}"
GROUP BY as_of_date, ticker
ORDER BY as_of_date, ticker
"""

# df = pd.read_gbq(holding_query)
print(holding_query)
