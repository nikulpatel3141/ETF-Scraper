"""
Module for helper functions to store retrieved data.

Defauly saving here is done using Pandas, so should work seamlessly between
local and cloud infrastructure.
"""
import logging
from datetime import date, datetime
from pathlib import Path
from itertools import product
from typing import Callable, List, Union
from traceback import format_exc
from multiprocessing.pool import ThreadPool

from tenacity import (
    retry,
    retry_if_not_exception_type,
    wait_random_exponential,
    stop_after_attempt,
    before_sleep_log,
)
import pandas as pd

from etf_scraper.base import InvalidParameterError
from etf_scraper.api import ETFScraper

DATE_FMT = "%Y_%m_%d"

logger = logging.getLogger(__name__)


def holdings_filename(ticker: str, holdings_date: date, file_extension: str) -> str:
    """Hardcoded default save name for files"""
    return f"{ticker}_{holdings_date.strftime(DATE_FMT)}{file_extension}"


def parse_holdings_filename(filename: str):
    """Go from a filename output by `holdings_filename` to the ticker + date + extension."""
    base, ext = filename.rsplit(".")
    ext_ = "." + ext

    ticker, date_str = base.split("_", 1)
    date_ = datetime.strptime(date_str, DATE_FMT).date()

    return ticker, date_, ext_


def list_unqueried_data(
    existing_files: list[str],
    expected_dates: List[date],
) -> List[date]:
    """Find the list of dates + tickers we are missing data for given an existing
    list of files. Expects filenames to be in the format returned by `holdings_filename`

    #TODO: allow users to pass their own filename parser
    """
    missing_data = []

    for x in existing_files:
        ticker, date_, _ = parse_holdings_filename(Path(x).name)
        if date_ not in expected_dates:
            continue
        missing_data.append((ticker, date_))

    return missing_data


def query_range(
    query_dates: list[date],
    query_tickers: list[str],
    etf_scraper: ETFScraper,
    save_func: Callable,
    num_threads=10,
):
    """Query the given dates + tickers using the passed ETFScraper and save the return
    value of save_func + # of returned holdings in a dict. Will return error messages
    in a dict if unable to query.

    Args:
        - save_func: a callable taking args (holdings_df, ticker, query_date) to save the output
        df. Should also return the save path if applicable.
        - num_threads: # of threads to parallelise over in Python's ThreadPool

    Returns: output as a dict with (ticker, date) as the keys.
    """
    assert num_threads >= 1

    # FIXME: put these retry parameters in a comfig file
    @retry(
        reraise=True,
        retry=retry_if_not_exception_type(InvalidParameterError),
        wait=wait_random_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(3),
        before_sleep=before_sleep_log(logger, logging.INFO),
    )
    def _query_holdings(ticker, query_date):
        holdings_df = etf_scraper.query_holdings(ticker, query_date)
        save_path = save_func(
            holdings_df=holdings_df, ticker=ticker, query_date=query_date
        )
        return save_path, len(holdings_df)

    def query_holdings(ticker, query_date):
        try:
            save_path, n_holdings = _query_holdings(ticker, query_date)
            return_rpt = {"save_path": save_path, "n_holdings": n_holdings}
        except Exception as e:
            return_rpt = {"error": format_exc(), "error_class": type(e).__name__}
        return return_rpt

    to_map = list(product(query_tickers, query_dates))

    with ThreadPool(num_threads) as p:
        from_pool = p.starmap(query_holdings, to_map)

    return dict(zip(to_map, from_pool))


def save_func(
    holdings_df: pd.DataFrame, ticker: str, query_date: Union[date, None], out_dir: str
):
    """Example function to pass to `query_range`. Saves output query date to
    a csv in out_dir (can also be local or a bucket on the cloud).

    If query_date not given then will infer the holdings date from the returned data.
    """
    if not query_date:
        query_date = holdings_df["as_of_date"].iloc[0]

    filename = holdings_filename(ticker, query_date, ".csv")
    out_path = Path(out_dir).joinpath(filename)
    logger.info(f"Saving holdings to {out_path}")
    holdings_df.to_csv(out_path)
    return out_path
