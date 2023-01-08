import logging
from typing import Union
from urllib.parse import urljoin
from io import StringIO
from datetime import date, datetime

import requests
import pandas as pd
import numpy as np

from etf_scraper.utils import check_missing_cols, safe_urljoin
from etf_scraper.base import ProviderListings, SecurityListing

logger = logging.getLogger(__name__)


class ISharesListings(ProviderListings):
    provider = "IShares"
    host = "https://www.ishares.com"
    listing_endpoint = (
        "/us/product-screener/product-screener-v3.1.jsn?dcrPath=/templatedata/config/product-screener-v3/"
        "data/en/us-ishares/ishares-product-screener-backend-config&siteEntryPassthrough=true"
    )
    ajax_endpoint = "1467271812596.ajax"

    response_mapping = {
        "fundName": "fund_name",
        "inceptionDate": "inception_date",
        "localExchangeTicker": "ticker",
        "cusip": "cusip",
        "isin": "isin",
        "aladdinAssetClass": "asset_class",
        "aladdinSubAssetClass": "subasset_class",
        "aladdinCountry": "country",
        "aladdinRegion": "region",
        "productPageUrl": "product_url",
        "portfolioId": "product_id",
        "totalNetAssets": "net_assets",
        "productView": "fund_type",
    }
    exp_cols = ["productPageUrl", "localExchangeTicker"]  # bare minimum to be returned

    holding_col_mapping = {
        "Ticker": "ticker",
        "Name": "name",
        "Sector": "sector",
        "Asset Class": "asset_class",
        "Market Value": "market_value",
        "Weight (%)": "weight",
        "Notional Value": "notional_value",
        "Shares": "amount",
        "Price": "price",
        "Location": "location",
        "Exchange": "exchange",
        "Currency": "currency",
        "FX Rate": "fx_rate",
        "Market Currency": "market_currency",
    }
    exp_holding_cols = ["Ticker", "Shares", "Market Value"]

    _fund_type_map = {"etf": "ETF", "mutualfund": "MF"}

    @classmethod
    def retrieve_listings(cls):
        listing_url = urljoin(cls.host, cls.listing_endpoint)
        resp = requests.get(listing_url)
        resp.raise_for_status()

        resp_df = pd.DataFrame(resp.json())

        check_missing_cols(cls.response_mapping, resp_df.index)
        check_missing_cols(cls.exp_cols, resp_df.index, raise_error=True)

        resp_df_ = (
            resp_df.reindex(cls.response_mapping).rename(index=cls.response_mapping).T
        )

        build_url = lambda x: safe_urljoin(cls.host, x)
        resp_df_.loc[:, "product_url"] = resp_df_["product_url"].apply(build_url)

        def parse_date(x):
            if date_ := x.get("r", None):
                try:
                    return datetime.strptime(str(date_), "%Y%m%d").date()
                except:
                    return pd.NaT
            return pd.NaT

        def parse_net(x):
            try:
                return float(x.get("r"))
            except:
                return np.nan

        parse_fund_type = lambda x: cls._fund_type_map.get(x[0].lower(), np.nan)

        resp_df_.loc[:, "inception_date"] = resp_df_["inception_date"].apply(parse_date)
        resp_df_.loc[:, "net_assets"] = resp_df_["net_assets"].apply(parse_net)
        resp_df_.loc[:, "fund_type"] = resp_df_["fund_type"].apply(parse_fund_type)
        resp_df_.loc[:, "provider"] = cls.provider
        return resp_df_.reset_index(drop=True)

    @classmethod
    def retrieve_holdings_(
        cls, ticker: str, product_url: str, holdings_date: Union[date, None]
    ):
        """Query for IShares product holdings
        Args:
        - holdings_date: date for holdings to query. If not given, then will
        query for the latest available holdings.
        """
        endpoint = safe_urljoin(product_url, cls.ajax_endpoint)
        req_params = {
            "fileType": "csv",
            "fileName": f"{ticker}_holdings",
            "dataType": "fund",
        }
        if holdings_date:
            req_params = {"asOfDate": holdings_date.strftime("%Y%m%d"), **req_params}

        logger.info(
            f"Querying {cls.provider} for {ticker} holdings as of {holdings_date}"
        )
        resp = requests.get(endpoint, params=req_params)
        resp.raise_for_status()

        raw_data = StringIO(resp.content.decode(encoding="UTF-8-SIG"))
        summary_data = [raw_data.readline().rstrip("\n") for _ in range(9)]

        date_info = {
            k.split(",", 1)[-1].strip("'\"")
            for k in summary_data
            if "Fund Holdings as of".lower() in k.lower()
        }
        if date_info == "-":
            raise ValueError(f"Found '-' as holdings date, no data received")
        elif len(date_info) != 1:
            raise ValueError(
                f"Was expecting an 'as of date' indicator, instead found: {date_info}"
            )

        logger.info(f"Found reported holdings date string {date_info}")
        logger.info("Attempting to parse holdings data")

        date_info = datetime.strptime(
            date_info.pop(), "%b %d, %Y"
        ).date()  # eg "Jan 03, 2022"

        if holdings_date and date_info != holdings_date:
            raise ValueError(
                f"Queried for date {holdings_date} but received holdings for {date_info} instead"
            )

        if summary_data[-1] != "\xa0":
            logger.warning(
                f"Was expecting \xa0 as the last line in the summary block."
                f" Found {summary_data[-1]} instead."
            )

        data_df = pd.read_csv(
            raw_data, thousands=",", na_values="-"
        )  # shouldn't need to skip any rows now

        check_missing_cols(cls.exp_holding_cols, data_df.columns, raise_error=True)
        data_df = data_df.rename(columns=cls.holding_col_mapping)

        data_df = data_df[~data_df["weight"].isna()]
        data_df.loc[:, "fund_ticker"] = ticker
        data_df.loc[:, "as_of_date"] = holdings_date
        return data_df

    @classmethod
    def retrieve_holdings(cls, sec_listing: SecurityListing, holdings_date: date):
        return cls.retrieve_holdings_(
            sec_listing.ticker, sec_listing.product_url, holdings_date
        )


class SSGAListings(ProviderListings):
    provider = "SSGA"
    host = "https://www.ssga.com"
    ssga_products = {"mf": "MF", "etfs": "ETF"}  # ignore 'cash' and 'strategies'

    ssga_web_url = "https://www.ssga.com/bin/v1/ssmp/fund/fundfinder?country=us&language=en&role=intermediary&product=@all&ui=fund-finder"

    ssga_web_resp_renaming = {
        "fundName": "fund_name",
        "fundTicker": "ticker",
        "fundUri": "product_url",
        "inceptionDate": "inception_date",
        "domicile": "region",
        "aum": "net_assets",
        "primaryExchange": "exchange",
    }
    exp_web_cols = ["fundTicker", "fundUri"]

    ssga_doc_url = "https://www.ssga.com/us/en/intermediary/ic/library-content/products/fund-data/etfs/us/spdr-product-data-us-en.xlsx"
    ssga_doc_mapping = {
        "Ticker": "ticker",
        "Name": "fund_name",
        "ISIN": "isin",
        "CUSIP": "cusip",
        "Inception Date": "inception_date",
        "Asset Class": "asset_class",
        "Primary Index": "benchmark",
    }
    exp_doc_cols = ["Ticker", "Asset Class"]

    @classmethod
    def query_ssga_fund_doc(cls) -> pd.DataFrame:
        """Query the document SSGA provides for ETF listings information such as
        asset class, exchange, isin. Not strictly necessary to query holdings information
        but useful if we want to filter on, eg Equity funds only.

        Note: doesn't cover mutual funds.
        """

        ssga_doc_req = requests.get(cls.ssga_doc_url)
        ssga_doc_req.raise_for_status()

        ssga_doc_df = pd.read_excel(ssga_doc_req.content, skiprows=1)
        check_missing_cols(cls.exp_doc_cols, ssga_doc_df.columns)

        ssga_doc_df = ssga_doc_df[~ssga_doc_df["Ticker"].isna()]
        ssga_doc_df_ = ssga_doc_df.reindex(columns=list(cls.ssga_doc_mapping)).rename(
            columns=cls.ssga_doc_mapping
        )
        return ssga_doc_df_

    @classmethod
    def query_ssga_webpage(cls) -> pd.DataFrame:
        resp = requests.get(cls.ssga_web_url)
        resp.raise_for_status()

        ssga_web_data = resp.json()

        ssga_web_data_ = ssga_web_data["data"]["funds"]
        ssga_web_data_df = pd.concat(
            [
                pd.DataFrame(ssga_web_data_[i]["datas"]).assign(fund_type=j)
                for i, j in cls.ssga_products.items()
            ]
        )
        check_missing_cols(cls.exp_web_cols, ssga_web_data_df.columns, raise_error=True)
        reindex_cols = [*cls.ssga_web_resp_renaming, "fund_type"]

        ssga_web_data_df_ = ssga_web_data_df.reindex(columns=reindex_cols).rename(
            columns=cls.ssga_web_resp_renaming
        )

        def parse_aum(x):
            try:
                return float(x[-1]) * 1e6  # reported in MM
            except:
                return np.nan

        def parse_date(x):
            try:
                return datetime.strptime(x[-1], "%Y-%m-%d")
            except:
                return pd.NaT

        ssga_web_data_df_.loc[:, "inception_date"] = ssga_web_data_df_[
            "inception_date"
        ].apply(parse_date)
        ssga_web_data_df_.loc[:, "net_assets"] = ssga_web_data_df_["net_assets"].apply(
            parse_aum
        )
        ssga_web_data_df_.loc[:, "product_url"] = ssga_web_data_df_[
            "product_url"
        ].apply(lambda x: safe_urljoin(cls.host, x))
        return ssga_web_data_df_

    @classmethod
    def retrieve_listings(cls):
        """Retrievs SSGA listings for ETFS and Mutual Funds"""
        ssga_doc_df = cls.query_ssga_fund_doc()
        ssga_web_data_df = cls.query_ssga_webpage()

        ssga_doc_df_ = ssga_doc_df[
            ["ticker", "asset_class", "cusip", "isin", "benchmark"]
        ]
        ssga_listings = ssga_web_data_df.merge(ssga_doc_df_, how="left", on="ticker")
        ssga_listings.loc[:, "provider"] = cls.provider
        return ssga_listings

    @classmethod
    def retrieve_holdings(
        cls, sec_listing: SecurityListing, holdings_date: Union[date, None]
    ) -> pd.DataFrame:

        if holdings_date:
            raise NotImplementedError(
                f"Can only query latest holdings (holdings_date=None) from SSGA"
            )
