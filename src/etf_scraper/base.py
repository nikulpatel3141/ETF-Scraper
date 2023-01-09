from datetime import date

import pandas as pd


class SecurityListing:
    """Describes a single security (ETF/Mutual Fund etc), including its product page"""

    fund_name: str
    ticker: str
    provider: str
    asset_class: str
    product_url: str
    fund_type: str  # ETF or Mutual Fund
    cusip: str
    isin: str
    product_id: str
    inception_date: date
    country: str
    region: str
    net_assets: float
    benchmark: str
    exchange: str


class FundHolding:
    """Describes a holding for a single fund (eg AAPL in SPY)"""

    fund_ticker: str
    as_of_date: date
    ticker: str
    isin: str
    sedol: str
    cusip: str
    name: str
    amount: float
    market_value: float
    notional_value: float  # != market_value eg for futures
    weight: float
    price: float
    location: str
    exchange: str
    currency: str
    fx_rate: float
    market_currency: str


class ProviderListings:
    """Encapsulate how to retrieve all listings from an ETF/Mutual Fund provider"""

    provider: str

    @classmethod
    def retrieve_listings(cls) -> pd.DataFrame:
        """Query the provider website for all listings and return as a dataframe.
        Should return with all columns in SecurityListing
        """
        raise NotImplementedError

    @classmethod
    def retrieve_holdings(
        cls, sec_listing: SecurityListing, holdings_date: date
    ) -> pd.DataFrame:
        """Query the provider website for the holdings of the given security
        and returns as a dataframe.

        Should return with all columns in FundHolding
        """
        raise NotImplementedError
