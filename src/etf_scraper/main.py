import warnings
from datetime import date
from functools import partial
from typing import Callable, List, Sequence, Any
import logging

from etf_scraper import ETFScraper
from etf_scraper.storage import (
    list_files,
    list_unqueried_data,
    query_hist_ticker_dates,
    default_save_func,
    format_hist_query_output,
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
) -> Sequence[date | None]:
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
):
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
        existing_files = list_files(save_dir, "." + out_fmt)

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

    save_func_ = partial(default_save_func, out_dir=save_dir, out_fmt=out_fmt)

    query_rpt = query_hist_ticker_dates(
        query_ticker_dates=to_query,
        etf_scraper=etf_scraper,
        save_func=save_func_,
        num_threads=num_threads,
    )
    return query_rpt
