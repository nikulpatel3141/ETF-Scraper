from enum import Enum
from datetime import date
from typing import NamedTuple, Union

import pandas as pd
import numpy as np


class InvalidParameterError(Exception):
    """An invalid combination of parameters was sent to an API.
    Most likely this is from requesting data for an invalid date
    """


class Provider(Enum):
    IShares = "IShares"
    SSGA = "SSGA"
    Vanguard = "Vanguard"
    Invesco = "Invesco"


class SecurityListing(NamedTuple):
    """Describes a single security (ETF/Mutual Fund etc), including its product page"""

    ticker: str
    provider: Provider
    fund_name: str = ""
    asset_class: str = ""
    subasset_class: str = ""
    product_url: str = ""
    fund_type: str = ""  # ETF or Mutual Fund # FIXME: should be an enum
    cusip: str = ""
    isin: str = ""
    product_id: str = ""
    inception_date: Union[date, None] = None
    country: str = ""
    region: str = ""
    net_assets: float = np.nan
    benchmark: str = ""
    exchange: str = ""


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
    weight: float
    security_type: str
    sector: str
    market_value: float
    notional_value: float  # != market_value eg for futures
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
    def _retrieve_holdings(
        cls, sec_listing: SecurityListing, holdings_date: date
    ) -> pd.DataFrame:
        """Query the provider website for the holdings of the given security
        and returns as a dataframe.

        Should return with all columns in FundHolding
        """
        raise NotImplementedError
