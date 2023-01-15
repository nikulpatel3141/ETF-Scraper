import logging
from typing import Union
from urllib.parse import urljoin
from io import StringIO
from datetime import date, datetime

import requests
import pandas as pd
import numpy as np

from etf_scraper.utils import (
    check_missing_cols,
    check_data_mismatch,
    safe_urljoin,
    set_numeric_cols,
    strip_str_cols,
)
from etf_scraper.base import Provider, ProviderListings, SecurityListing

logger = logging.getLogger(__name__)


def _check_exp_provider(given, exp, class_name):
    if given != exp:
        raise ValueError(
            f"Provider should be {exp}, not {given} for class {class_name}"
        )


class ISharesListings(ProviderListings):
    provider = Provider.IShares.value
    host = "https://www.ishares.com"
    listing_endpoint = (
        "/us/product-screener/product-screener-v3.1.jsn?dcrPath=/templatedata/config/product-screener-v3/"
        "data/en/us-ishares/ishares-product-screener-backend-config&siteEntryPassthrough=true"
    )
    ajax_endpoint = "1467271812596.ajax"

    listing_resp_mapping = {
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

        check_missing_cols(cls.listing_resp_mapping, resp_df.index)
        check_missing_cols(cls.exp_cols, resp_df.index, raise_error=True)

        resp_df_ = (
            resp_df.reindex(cls.listing_resp_mapping)
            .rename(index=cls.listing_resp_mapping)
            .T
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
    def _parse_holdings_resp(cls, resp_content):
        raw_data = StringIO(resp_content)
        summary_data = [raw_data.readline().rstrip("\n") for _ in range(9)]

        as_of_date = {
            k.split(",", 1)[-1].strip("'\"")
            for k in summary_data
            if "Fund Holdings as of".lower() in k.lower()
        }

        if len(as_of_date) != 1:
            raise ValueError(
                f"Was expecting an 'as of date' indicator, instead found: {as_of_date}"
            )

        as_of_date = list(as_of_date)[0]

        if as_of_date == "-":
            raise ValueError(f"Found '-' as holdings date -> no data returned")

        logger.info(f"Found reported holdings date string {as_of_date}")
        logger.info("Attempting to parse holdings data")

        as_of_date = datetime.strptime(
            as_of_date, "%b %d, %Y"
        ).date()  # eg "Jan 03, 2022"

        if summary_data[-1] != "\xa0":
            logger.warning(
                f"Was expecting \xa0 as the last line in the summary block."
                f" Found {summary_data[-1]} instead."
            )

        holdings_df = pd.read_csv(
            raw_data, thousands=",", na_values="-"
        )  # shouldn't need to skip any rows now

        check_missing_cols(cls.exp_holding_cols, holdings_df.columns, raise_error=True)

        holdings_df = holdings_df.rename(columns=cls.holding_col_mapping)
        holdings_df = holdings_df[~holdings_df["weight"].isna()]

        strip_str_cols(holdings_df, ["ticker"])
        set_numeric_cols(
            holdings_df, ["amount", "weight", "market_value", "price", "notional_value"]
        )

        return holdings_df, as_of_date

    @classmethod
    def retrieve_holdings(
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

        holdings_df, as_of_date = cls._parse_holdings_resp(
            resp.content.decode(encoding="UTF-8-SIG")
        )

        if holdings_date:
            check_data_mismatch(
                holdings_date, as_of_date, "holdings date", raise_error=True
            )

        holdings_df.loc[:, "fund_ticker"] = ticker
        holdings_df.loc[:, "as_of_date"] = as_of_date
        return holdings_df

    @classmethod
    def _retrieve_holdings(cls, sec_listing: SecurityListing, holdings_date: date):
        _check_exp_provider(sec_listing.provider, cls.provider, cls.__name__)
        return cls.retrieve_holdings(
            sec_listing.ticker, sec_listing.product_url, holdings_date
        )


class SSGAListings(ProviderListings):
    provider = Provider.SSGA.value
    host = "https://www.ssga.com"

    # FIXME: values should be enum
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

    etf_holdings_url = (
        "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/"
        "etfs/us/holdings-daily-us-en-{}.xlsx"
    )

    etf_holdings_col_map = {
        "Name": "name",
        "Ticker": "ticker",
        "Identifier": "cusip",
        "SEDOL": "sedol",
        "Weight": "weight",
        "Sector": "Sector",
        "Shares Held": "amount",
        "Local Currency": "local_currency",
    }

    @classmethod
    def _query_ssga_fund_doc(cls) -> pd.DataFrame:
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
    def _query_ssga_webpage(cls) -> pd.DataFrame:
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
        ssga_doc_df = cls._query_ssga_fund_doc()
        ssga_web_data_df = cls._query_ssga_webpage()

        ssga_doc_df_ = ssga_doc_df[
            ["ticker", "asset_class", "cusip", "isin", "benchmark"]
        ]
        ssga_listings = ssga_web_data_df.merge(ssga_doc_df_, how="left", on="ticker")
        ssga_listings.loc[:, "provider"] = cls.provider
        return ssga_listings

    @classmethod
    def _parse_holdings_resp(cls, resp_content):
        """Parse SSGA ETF holdings Excel
        These contain a preamble (with as of date, ticker etc) and the main
        holdings.

        Returns: a df of the holdings, and a date object representing the reported
        as of date and ticker
        """
        resp_df = pd.read_excel(resp_content)
        header_row = resp_df[resp_df.iloc[:, 0] == "Name"].index[0]
        preamble = resp_df.iloc[:header_row, :2].set_index(resp_df.columns[0]).squeeze()
        preamble.index = [str(x).rstrip(":").strip() for x in preamble.index]
        ticker = preamble["Ticker Symbol"]

        as_of_val = preamble["Holdings"]
        as_of_val = as_of_val.lower().split("as of")[-1].strip()
        as_of_date = datetime.strptime(as_of_val, "%d-%b-%Y").date()

        # return resp_df
        holdings_df = resp_df.iloc[header_row + 1 :]
        holdings_df.columns = resp_df.iloc[header_row].values
        holdings_df = holdings_df[~holdings_df["Ticker"].isna()]

        check_missing_cols(["Ticker", "Shares Held"], holdings_df.columns)

        holdings_df = holdings_df.reindex(
            columns=list(cls.etf_holdings_col_map)
        ).rename(columns=cls.etf_holdings_col_map)

        strip_str_cols(holdings_df, ["ticker"])
        set_numeric_cols(holdings_df, ["weight", "amount"])

        return holdings_df, as_of_date, ticker

    @classmethod
    def retrieve_holdings(cls, ticker: str) -> pd.DataFrame:
        holdings_url = cls.etf_holdings_url.format(ticker.lower())
        resp = requests.get(holdings_url)
        resp.raise_for_status()

        holdings_df, resp_holdings_date, resp_ticker = cls._parse_holdings_resp(
            resp.content
        )

        logger.info(f"Found response as of date {resp_holdings_date} for {ticker}")

        if ticker.upper() != resp_ticker.upper():
            raise ValueError(
                f"Response ticker {resp_ticker} doesn't match the query ticker {ticker}"
            )

        holdings_df.loc[:, "fund_ticker"] = ticker
        holdings_df.loc[:, "as_of_date"] = resp_holdings_date
        return holdings_df

    @classmethod
    def _retrieve_holdings(
        cls, sec_listing: SecurityListing, holdings_date: Union[date, None]
    ) -> pd.DataFrame:
        _check_exp_provider(sec_listing.provider, cls.provider, cls.__name__)

        if holdings_date:
            raise NotImplementedError(
                f"Can only query latest holdings (holdings_date=None) from SSGA"
            )

        if sec_listing.fund_type != "ETF":
            raise NotImplementedError(
                f"Can only retrieve SSGA ETF holdings, not {sec_listing.fund_type}"
            )

        return cls.retrieve_holdings(sec_listing.ticker)


class VanguardListings(ProviderListings):
    provider = Provider.Vanguard.value
    req_user_header = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    }  # need to pass otherwise requests will be denied
    listing_endpoint = (
        "https://investor.vanguard.com/investment-products/list/funddetail"
    )
    listing_resp_mapping = {
        "ticker": "ticker",
        "cusip": "cusip",
        "longName": "fund_name",
        "inceptionDate": "inception_date",
        "fundId": "product_id",
        "style": "asset_class",
        "type": "subasset_class",
    }
    fund_url = "https://advisors.vanguard.com/investments/products/{}"

    holding_col_mapping = {
        "effectiveDate": "as_of_date",
        "bticker": "ticker",
        "sedol": "sedol",
        "CUSIP": "cusip",
        "name": "name",
        "quantity": "amount",
        "mktValPercent": "weight",
        "parentIssueTypeName": "security_type",
        "sector": "sector",
        "market": "exchange",
        "marketVal": "market_value",
        "countryOfRiskCode": "location",
    }

    @classmethod
    def holdings_endpoint(cls, product_id: str) -> str:
        """Endpoint to send a GET request to for querying holdings.

        Note: can also append `as-of-date=YYYY-MM-DD` in the payload to request a specific
        date, but Vanguard will only return end of month holdings.
        """
        return (
            "https://eds.ecs.gisp.c1.vanguard.com/eds-eip-distributions-service/"
            f"holdings/holding-details-history/{product_id}.json"
        )

    @classmethod
    def retrieve_listings(cls):
        """
        #TODO: missing important info: eg AUM, country etf - not possible to get without visiting individual
        fund pages
        #FIXME: duplicates via multiple share classes, eg VFIAX, VFFSX, VOO
        """
        resp = requests.get(cls.listing_endpoint, headers=cls.req_user_header)
        resp.raise_for_status()
        fund_data = resp.json()
        fund_list = [x["profile"] for x in fund_data["fund"]["entity"]]
        fund_df = pd.DataFrame(fund_list)

        check_missing_cols(["ticker"], fund_df.columns, raise_error=True)
        check_missing_cols(list(cls.listing_resp_mapping), fund_df.columns)

        fund_df_ = fund_df.dropna(subset="ticker").rename(
            columns=cls.listing_resp_mapping
        )

        def map_fund_type(fund_row: pd.Series):
            if fund_row["isETF"]:
                return "ETF"
            elif fund_row["isMutualFund"]:
                return "MF"
            return np.nan

        fund_types = fund_df_.apply(map_fund_type, axis=1)

        fund_df_ = fund_df_.reindex(columns=list(cls.listing_resp_mapping.values()))

        fund_df_.loc[:, "fund_type"] = fund_types

        map_to_url = lambda x: cls.fund_url.format(x.upper())
        fund_df_.loc[:, "product_url"] = fund_df_["ticker"].apply(map_to_url)

        def parse_inception_date(x):
            try:
                return datetime.fromisoformat(x).date()
            except:
                return pd.NaT

        fund_df_.loc[:, "inception_date"] = fund_df_["inception_date"].apply(
            parse_inception_date
        )
        fund_df_.loc[:, "provider"] = cls.provider

        return fund_df_

    @classmethod
    def _parse_holdings_resp(cls, holdings_resp):
        """Parse the response from (a hidden) Vanguard API for holdings data"""
        assert len(holdings_resp) == 1
        holdings_resp_ = holdings_resp[0]

        ret_item_id = holdings_resp_["portId"]
        holdings_df_raw = pd.DataFrame(holdings_resp_["holdingDetailItem"])

        check_missing_cols(
            ["effectiveDate", "bticker", "quantity"],
            holdings_df_raw.columns,
            raise_error=True,
        )
        check_missing_cols(list(cls.holding_col_mapping), holdings_df_raw.columns)

        holdings_df = holdings_df_raw.reindex(
            columns=list(cls.holding_col_mapping)
        ).rename(columns=cls.holding_col_mapping)
        holdings_df.loc[:, "as_of_date"] = pd.to_datetime(holdings_df["as_of_date"])

        strip_str_cols(holdings_df, ["ticker"])
        set_numeric_cols(holdings_df, ["amount", "market_value", "weight"])

        return holdings_df, ret_item_id

    @classmethod
    def retrieve_holdings(
        cls, fund_ticker: str, product_id: str, holdings_date: date
    ) -> pd.DataFrame:
        """
        Args:
        - fund_ticker: ETF/MF ticker. This is only for appending to the returned dataframe
        for consistency. It doesn't validate this corresponds to the given product_id.
        - product_id: the internal ID Vanguard uses for their products, eg 0968 -> VOO

        Note: ticker may be missing, eg for short-term reserve positions
        """
        url = cls.holdings_endpoint(product_id)
        payload = {"as-of-date": holdings_date.strftime("%Y-%m-%d")}

        resp = requests.get(
            url, params=payload, headers=VanguardListings.req_user_header
        )
        resp.raise_for_status()
        resp_data = resp.json()

        if not resp_data:  # will silently return no data
            raise ValueError(
                f"No Vanguard data returned for product_id: {product_id}, date: {holdings_date}"
            )

        holdings_df, ret_product_id = cls._parse_holdings_resp(resp_data)
        check_data_mismatch(product_id, ret_product_id, "product id", raise_error=True)

        ret_holdings_date = holdings_df["as_of_date"].drop_duplicates()

        if len(ret_holdings_date) > 1:
            raise ValueError(f"Multiple holding dates returned: {ret_holdings_date}")

        ret_holdings_date = ret_holdings_date[0].date()
        check_data_mismatch(
            holdings_date, ret_holdings_date, "holdings date", raise_error=True
        )
        holdings_df.loc[:, "fund_ticker"] = fund_ticker

        return holdings_df

    @classmethod
    def _retrieve_holdings(
        cls, sec_listing: SecurityListing, holdings_date: date
    ) -> pd.DataFrame:
        """Retrieve Vanguard ETF/MF holdings. Will only return data for month end
        holdings dates.
        """
        _check_exp_provider(sec_listing.provider, cls.provider, cls.__name__)
        return cls.retrieve_holdings(
            sec_listing.ticker, sec_listing.product_id, holdings_date
        )


class InvescoListings(ProviderListings):
    """TODO: Currently only works for ETF data (ie we are missing Invesco MFs)"""

    provider = Provider.Invesco.value

    date_fmt = "%m/%d/%Y"
    listings_url = (
        "https://www.invesco.com/us/financial-products/etfs/performance/prices/main/"
        "performance/0?audienceType=Advisor&action=download"
    )
    listings_resp_mapping = {
        "Name": "fund_name",
        "Ticker": "ticker",
        "Inception_Date": "inception_date",
        "Index_Ticker": "benchmark",
        "CUSIP": "cusip",
        "ISIN": "isin",
        "Exchange": "exchange",
    }

    item_url = (
        "https://www.invesco.com/us/financial-products/etfs/"
        "product-detail?audienceType=Investor&ticker={}"
    )

    holdings_url = (
        "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/"
        "0?audienceType=Investor&action=download&ticker={}"
    )
    holdings_resp_mapping = {
        "Fund Ticker": "fund_ticker",
        "Security Identifier": "cusip",
        "Holding Ticker": "ticker",
        "Shares/Par Value": "amount",
        "MarketValue": "market_value",
        "Weight": "weight",
        "Name": "name",
        "Class of Shares": "security_type",  # FIXME: is this accurate???
        "Sector": "sector",
        "Date": "as_of_date",
    }

    @classmethod
    def _parse_date(cls, date_str: str) -> date:
        return datetime.strptime(date_str, cls.date_fmt).date()

    @classmethod
    def retrieve_listings(cls) -> pd.DataFrame:
        """Retrieve all ETF listings from Invesco"""
        resp = requests.get(cls.listings_url)
        resp.raise_for_status()

        resp_buffer = StringIO(resp.content.decode())
        listings_df = pd.read_csv(resp_buffer, skiprows=5)

        check_missing_cols(["Ticker"], listings_df, raise_error=True)
        check_missing_cols(list(cls.listings_resp_mapping), listings_df)

        listings_df_ = listings_df.reindex(
            columns=list(cls.listings_resp_mapping)
        ).rename(columns=cls.listings_resp_mapping)
        listings_df_.loc[:, "inception_date"] = listings_df_["inception_date"].apply(
            cls._parse_date
        )
        listings_df_.loc[:, "product_url"] = listings_df_["ticker"].apply(
            lambda x: cls.item_url.format(x)
        )
        listings_df_.loc[:, "fund_type"] = "ETF"
        listings_df_.loc[:, "provider"] = cls.provider

        return listings_df_

    @classmethod
    def _parse_holdings_resp(cls, holdings_resp):
        """Parse the CSVs Invesco provide for holdings data"""
        holdings_df = pd.read_csv(StringIO(holdings_resp.decode()), thousands=",")
        check_missing_cols(
            ["Holding Ticker", "Shares/Par Value"],
            holdings_df.columns,
            raise_error=True,
        )
        check_missing_cols(cls.holdings_resp_mapping, holdings_df.columns)

        holdings_df_ = holdings_df.reindex(
            columns=list(cls.holdings_resp_mapping)
        ).rename(columns=cls.holdings_resp_mapping)

        holdings_df_.loc[:, "as_of_date"] = holdings_df_["as_of_date"].apply(
            cls._parse_date
        )
        strip_str_cols(holdings_df_, ["ticker", "fund_ticker"])
        set_numeric_cols(holdings_df_, ["amount", "market_value", "weight"])
        return holdings_df_

    @classmethod
    def retrieve_holdings(cls, ticker: str):
        """Retrieve the latest holdings for the given ETF ticker"""
        url = cls.holdings_url.format(ticker.upper())
        resp = requests.get(url)
        resp.raise_for_status()

        holdings_df = cls._parse_holdings_resp(resp.content)

        holdings_date = holdings_df["as_of_date"].drop_duplicates()
        if len(holdings_date) > 1:
            raise ValueError(
                f"Found multiple holdings dates, was expecting one: {list(holdings_date)}"
            )

        fund_ticker = holdings_df["fund_ticker"].drop_duplicates()
        if len(fund_ticker) > 1:
            raise ValueError(
                f"Found multiple fund tickers, was expecting one ({ticker}): {list(fund_ticker)}"
            )

        check_data_mismatch(ticker, fund_ticker[0], "fund ticker")

        return holdings_df

    @classmethod
    def _retrieve_holdings(
        cls, sec_listing: SecurityListing, holdings_date: Union[None, date] = None
    ) -> pd.DataFrame:
        """Retrieve Invesco ETF holdings data"""
        _check_exp_provider(sec_listing.provider, cls.provider, cls.__name__)

        if holdings_date:
            raise ValueError(f"Can only retrieve the latest holdings from Invesco")

        return cls.retrieve_holdings(sec_listing.ticker)
