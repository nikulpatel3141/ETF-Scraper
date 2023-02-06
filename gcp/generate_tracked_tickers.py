"""Generate a subset of ETF tickers to track.

By default we subset on the top 100 US Equity ETFs by market cap.

Note: it would be better to subset on top ETFs by traded value since we are tracking
flows, but this is (slightly) harder to retrieve. Market cap should be a good proxy anyway.
"""

import pandas as pd

from etf_scraper import query_listings


def subset_ishares(ishares_listings) -> pd.DataFrame:
    return ishares_listings[
        (ishares_listings["asset_class"] == "Equity")
        & (ishares_listings["fund_type"] == "ETF")
        & ishares_listings["region"].isin(["North America", "Global"])
    ]


def subset_ssga(ssga_listings) -> pd.DataFrame:
    return ssga_listings[
        (ssga_listings["fund_type"] == "ETF")
        & ssga_listings["region"].isin(["US"])
        & (ssga_listings["asset_class"] == "Equity")
    ]


def subset_


def main():
    listings = query_listings()
    listings_dict = dict(list(listings.groupby("provider")))
