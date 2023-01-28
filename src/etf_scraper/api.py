import logging
from functools import lru_cache
from datetime import date
from multiprocessing.pool import ThreadPool
from typing import List, Union

import dateutil.parser
import pandas as pd

from etf_scraper.config import LISTINGS_FILE
from etf_scraper.base import Provider, SecurityListing
from etf_scraper.scrapers import (
    ISharesListings,
    InvescoListings,
    VanguardListings,
    SSGAListings,
)

SCRAPERS = {
    k.provider: k
    for k in [
        ISharesListings,
        InvescoListings,
        VanguardListings,
        SSGAListings,
    ]
}

logger = logging.getLogger(__name__)


def query_listings(providers: Union[None, List[Provider]] = None) -> pd.DataFrame:
    """Query provider listings for the providers given (or all if none given)
    and return as a df
    """
    logger.debug("Querying for provider listings")

    if not providers:
        providers = list(SCRAPERS)
        logger.info(
            f"No providers given, defaulting to all providers: {', '.join(providers)}"
        )

    # TODO: add retry decorator + try/except block
    def query_listings_(provider: Provider) -> pd.DataFrame:
        logger.info(f"Querying for fund listings from {provider}")
        listings_df = SCRAPERS[provider].retrieve_listings()
        logger.info(f"Retrieved {len(listings_df)} listings from {provider}")
        return listings_df

    n_threads = len(
        providers
    )  # FIXME: don't see any problems but it doesn't seem sensible

    with ThreadPool(n_threads) as p:
        from_pool = p.map(query_listings_, providers)

    # TODO: check for duplicate tickers
    return pd.concat(from_pool)


@lru_cache
def load_listings() -> pd.DataFrame:
    """Load the listings file shipped with the etf_scraper module"""
    logging.debug(f"Loading default listings file from {LISTINGS_FILE}")
    return pd.read_csv(LISTINGS_FILE)


class ETFScraper:
    def __init__(self, listings_df: Union[pd.DataFrame, None] = None):
        if listings_df is None:
            listings_df = load_listings()

        self.listings_df = listings_df.drop_duplicates()

    def _find_sec_listing(self, ticker: str) -> SecurityListing:
        """Generate a SecurityListing for the given ticker.

        Raises:
        - ValueError: if there are multiple/no securities corresponding to the given ticker.
        """
        sec_row = self.listings_df[self.listings_df["ticker"] == ticker]

        if len(sec_row) == 0:
            raise ValueError(f"No listings found for ticker {ticker}")
        elif len(sec_row) > 1:
            raise ValueError(
                f"Multiple listings found for ticker: {sec_row.to_string()}"
            )  # FIXME: better logging, but probably won't happen anyway

        sec_dict = sec_row.iloc[0].to_dict()
        return SecurityListing(**sec_dict)

    def query_holdings(self, ticker: str, holdings_date: Union[date, str, None] = None):
        """Query for holdings for the given ticker + holdings_date.

        Args:
        - ticker: the fund ticker to query. Must be in self.listings_df.
        - holdings date: the holdings date to query. If blank then will query for the latest
        possible holdings. If given as a string, then attempt to parse using
        dateutil.parser.parse.

        #FIXME: could have provider = None: Union[None, Provider] as an arg to help reduce
        # duplicate tickers. As of Jan 2023 this isn't a problem.
        """
        if holdings_date and isinstance(holdings_date, str):
            holdings_date = pd.to_datetime(holdings_date).date()

        if not ticker:
            raise ValueError("Need to pass a ticker to scrape it")

        sec_listing = self._find_sec_listing(ticker)

        date_log_str = "latest" if not holdings_date else holdings_date
        logger.debug(
            f"Querying for {sec_listing.ticker} holdings as of {date_log_str}"
            f" from {sec_listing.provider}"
        )
        scraper = SCRAPERS[sec_listing.provider]
        return scraper._retrieve_holdings(sec_listing, holdings_date)
