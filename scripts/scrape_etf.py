"""
Query the given ticker 
"""

import argparse
import logging
from io import StringIO

from etf_scraper import ETFScraper

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        prog="Single ETF Scraper",
        description="Query for a single ticker's holdings for a single date and outputs as a csv to stdout",
    )
    parser.add_argument("--ticker", type=str)
    parser.add_argument("--date", type=str, default="")
    return vars(parser.parse_args())


def main():
    args = parse_args()

    ticker = args["ticker"]
    holdings_date = args["date"]

    es = ETFScraper()

    holdings_df = es.query_holdings(
        ticker=ticker,
        holdings_date=holdings_date,
    )

    out_buffer = StringIO()
    holdings_df.to_csv(out_buffer, index=False)

    print(out_buffer.getvalue())


if __name__ == "__main__":
    main()
