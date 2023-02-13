"""Top level functions to parse user input, scrape ETF holdings and save it locally or
in a cloud storage bucket.
"""

from pathlib import Path
import warnings
from datetime import date
from functools import partial
from typing import List, Sequence, Union
import logging

from etf_scraper import ETFScraper
from etf_scraper.storage import (
    list_files,
    list_unqueried_data,
    query_hist_ticker_dates,
    default_save_func,
    SaveFormat,
)
from etf_scraper.utils import get_interval_query_dates

warnings.simplefilter(action="ignore", category=FutureWarning)

logger = logging.getLogger(__name__)


def _log_exit(start_date, end_date, month_ends, trading_days, exchange=None):
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "month_ends": month_ends,
        "trading_days": trading_days,
    }
    if trading_days:
        params["exchange"] = exchange
    log_str = "\n".join([f"{k}: {v}" for k, v in params.items()])
    logger.warning("There are no dates to query with parameters:\n " f"{log_str}")


def parse_query_date_range(
    start_date: str,
    end_date: str,
    month_ends: bool = False,
    trading_days: bool = False,
    overwrite: bool = False,
    exchange: str = "NYSE",
) -> Sequence[Union[date, None]]:
    """Parse user input to generate a list of query dates"""

    if not start_date:
        if end_date:
            raise ValueError(f"start_date missing, but end_date {end_date} given")
        elif overwrite:
            raise ValueError(
                f"Cannot overwrite without a start + end date (we don't know what to overwrite)"
            )

        query_dates = [None]
    else:
        if not end_date:
            logger.warning(
                f"Only start date {start_date} given but not an end date, so setting end_date={start_date}"
            )
            end_date = start_date
        else:
            end_date = end_date

        query_dates = get_interval_query_dates(
            start_date,
            end_date,
            month_ends,
            trading_days,
            exchange,
        )
    return query_dates


def scrape_holdings(
    tickers: List[str],
    start_date: str,
    end_date: str,
    month_ends: bool,
    trading_days: bool,
    overwrite: bool,
    save_dir: str,
    out_fmt: SaveFormat,
    num_threads: int = 10,
    exchange: str = "NYSE",
    existing_file_dir: str = "",
) -> dict:
    """Scrape ETF holdings for the given tickers + date range specified
    and saves it to save_dir using etf_scraper.storage.default_save_func.

    Args:
    - start_date: date to start querying from. If this + end_date blank then
    query for latest holdings.
    - month_ends: if True then only query for month ends
    - trading_days: if True then only query for trading days. If month_ends=True
    then only query for month end trading days.
    - overwrite: if True then ignore all existing files and requery all requested
    dates + tickers.
    - exchange: only relevant when trading_days=True. Used to determine the
    exchange calendar to subset on.
    - existing_file_dir: recursively list existing files to determine remaining
    tickers + dates to query. If not given, then save_dir is used instead.

    Returns: a dict logs for each ticker + date queried. See
    etf_scraper.storage.default_save_func for the log format
    """
    if not existing_file_dir:
        existing_file_dir = save_dir

    etf_scraper = ETFScraper()
    query_dates = parse_query_date_range(
        start_date,
        end_date,
        month_ends,
        trading_days,
        overwrite,
        exchange,
    )

    if not len(query_dates):
        _log_exit(start_date, end_date, month_ends, trading_days, exchange)
        return

    if overwrite:
        logger.warning(f"Overwriting existing data (if any) at {save_dir}")
        existing_files = []
    else:
        existing_files = list_files(existing_file_dir, "." + out_fmt)
        logger.info(f"Found {len(existing_files)} .{out_fmt} files in {save_dir}")

    if list(query_dates) != [None]:
        to_query = list_unqueried_data(
            existing_files,
            query_dates,
            tickers,
        )
        unique_tickers = {x[0] for x in to_query}
        if to_query:
            logger.info(
                f"Querying {len(to_query)} holdings files for "
                f"{len(query_dates)} dates and {len(unique_tickers)} tickers"
            )
        else:
            logger.info(
                f"Nothing to query (the requested files may already exist at {save_dir})"
            )
    else:
        logger.info(f"Querying latest holdings for {len(tickers)} ticker(s)")
        to_query = [(ticker, None) for ticker in tickers]

    # empty if overwrite = False
    existing_filenames = [
        Path(x).name for x in existing_files
    ]  # FIXME: repetition from list_unqueried_data
    save_func_ = partial(
        default_save_func,
        out_dir=save_dir,
        out_fmt=out_fmt,
        existing_filenames=existing_filenames,
    )
    query_rpt = query_hist_ticker_dates(
        query_ticker_dates=to_query,
        etf_scraper=etf_scraper,
        save_func=save_func_,
        num_threads=num_threads,
    )
    return query_rpt
