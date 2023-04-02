"""Calculate flows and saves as nicely formatted HTML tables in GCS"""

import logging
import os
import json
from datetime import datetime, date
from pathlib import Path

import pandas as pd
from google.cloud import storage


PROJECT_ID = os.getenv("PROJECT_ID")
SAVE_URI = os.getenv("SAVE_URI")

DATASET_NAME = "etf_holdings"
HOLDINGS_TABLE_NAME = "etf_holdings"

FLOW_DELAY = 2  # cutoff calculations to 2 business days ago
LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", 21))
UNIVERSE_FUND = "IVV"

# for filtering obviously wrong flows
OUTLIER_QUANTILE = int(os.getenv("OUTLIER_QUANTILE", 0.9))
OUTLIER_MULT = int(os.getenv("OUTLIER_MULT", 50))

HOLDINGS_TABLE = f"`{PROJECT_ID}.{DATASET_NAME}.{HOLDINGS_TABLE_NAME}`"
_FMT_TICKER = """REPLACE(REPLACE(ticker, " ", ""), ".", "")"""

TBL_BORDER = "1px"
TBL_STYLES = [
    {"selector": "tr:hover", "props": "background-color: yellow"},
    {"selector": "th", "props": "background-color: #346eeb"},
    {"selector": "th, td", "props": "padding: 4px; border: 1px solid grey"},
]
TBL_ATTRS = "style='border-collapse: collapse;'"
BAR_COLORS = ["#d65f5f", "#5fba7d"]
BAR_PROPS = "opacity: 0.8; width: 10em; text-align: center;"

NUM_TICKER_SUBSET = 10


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(filename)16s:%(lineno)4d:%(levelname)8s] - %(message)s",
)

logger = logging.getLogger(__name__)


def parse_uri(uri: str):
    """Split a (GCS) URI to bucket + path, eg
    "gs://bucket/path/to/file.json" -> ("bucket", "path/to/file.json")
    """
    _, bucket, *blob = Path(uri).parts
    return bucket, "/".join(blob)


def gcs_write(bucket_name, blob_name, to_write: str):
    """Write and read a blob from GCS using file-like IO"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("w") as f:
        f.write(to_write)


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
        SELECT as_of_date, ticker, sector, AVG(price) price
        FROM raw_holdings
        WHERE
            fund_ticker = "{UNIVERSE_FUND}" AND
            sector NOT IN ('Cash and/or Derivatives')
        GROUP BY as_of_date, ticker, sector
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
    SELECT flows.ticker, sector, SUM(amount_diff * price) flow
    FROM flows LEFT JOIN subset_prices
    ON
    flows.as_of_date = subset_prices.as_of_date AND
    flows.ticker = subset_prices.ticker 
    WHERE flows.as_of_date BETWEEN "{lookback_date}" AND "{cur_holdings_date}"
    GROUP BY flows.ticker, sector
    ORDER BY flow, flows.ticker
    """


def fmt_curr(x: float) -> str:
    """Format the input as a thousands-separated int in millions"""
    try:
        return f"{x//1e6:,.0f}"
    except:
        return ""


def fmt_data(flows: pd.Series):
    col = flows.index.name.title()
    flows_ = flows.rename("Flow ($MM)").rename_axis(col)
    flows_styled = (
        flows_.to_frame()
        .style.set_table_styles(TBL_STYLES)
        .set_table_attributes(TBL_ATTRS)
        .format(fmt_curr)
        .bar(color=BAR_COLORS, props=BAR_PROPS)
    )
    return flows_styled


def top_tail_df(df, n: int):
    """Return top + bottom n rows of the given df"""
    if len(df) <= 2 * n:
        return df
    return pd.concat([df.head(n), df.tail(n)])


def main():
    """Calculate flows and saves as nicely formatted json in GCS
    #FIXME: refactor
    """
    logger.info(
        f"Attempting to calculate flows and save formatted output to {SAVE_URI}"
    )
    bucket, blob = parse_uri(SAVE_URI)

    cur_holdings_date, lookback_date, buffer_lookback_date = get_query_dates()
    flow_query = generate_flow_query(
        cur_holdings_date, lookback_date, buffer_lookback_date
    )
    logger.info(f"Querying flows from {lookback_date} to {cur_holdings_date}")
    df = pd.read_gbq(flow_query)
    df_ = df.dropna()

    abs_flow_limit = df_["flow"].abs().quantile(OUTLIER_QUANTILE) * OUTLIER_MULT
    df_ = df_[df_["flow"].abs() < abs_flow_limit]

    if len(df_) < len(df):
        logger.warning(f"Dropped {len(df_)-len(df)} invalid flows")

    logger.info("Calculating aggregated flows")
    grp_dfs = {
        k: df_.groupby(k)["flow"].sum().sort_values(ascending=False)
        for k in ["sector", "ticker"]
    }
    grp_dfs["total"] = pd.Series(
        grp_dfs["ticker"].sum(), index=pd.Index([""], name="Total")
    )
    logger.info("Formatting data")
    subset_ticker_df = top_tail_df(grp_dfs["ticker"], NUM_TICKER_SUBSET)
    styled_data = {k: fmt_data(v) for k, v in grp_dfs.items()}
    styled_data = {
        "total": fmt_data(grp_dfs["total"]),
        "ticker": fmt_data(subset_ticker_df),
        "sector": fmt_data(grp_dfs["sector"]),
    }

    styled_data_html = {
        **{k: styler.to_html() for k, styler in styled_data.items()},
        "as_of_date": str(cur_holdings_date),
    }
    styled_data_html_ = json.dumps(styled_data_html)

    logger.info(f"Attempting to save output to bucket {bucket}, blob {blob}")
    gcs_write(bucket, blob, styled_data_html_)

    logger.info("Done!")


if __name__ == "__main__":
    main()
