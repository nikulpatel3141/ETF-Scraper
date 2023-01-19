"""
Module for helper functions to store retrieved data.

This is based on fsspec (https://github.com/fsspec/filesystem_spec) so
should work seamlessly between local and cloud infrastructure.
"""
import os
from datetime import date
from typing import List

from fsspec.spec import AbstractFileSystem

DATE_FMT = "%Y_%m_%d"


def save_filename(ticker: str, holdings_date: date, file_extension: str) -> str:
    """Hardcoded default save name for files

    Args:
    - ticker: the queried ticker, eg SPY
    - holdings_date: holdings data for the corresponding data
    - file_extension: eg ".csv", ".parquet" etc
    """
    return f"{ticker}_{holdings_date.strftime(DATE_FMT)}{file_extension}"


def list_unqueried_dates(
    file_system: AbstractFileSystem,
    save_dir: str,
    ticker: str,
    expected_dates: List[date],
    file_extension: str,
) -> List[date]:
    """Find the list of dates which we expect to have data for but don't for a single ticker"""
    existing_files = [os.path.basename(x) for x in file_system.ls(save_dir)]
    save_filename_ = lambda x: save_filename(ticker, x, file_extension)

    missing_dates = [
        date_ for date_ in expected_dates if save_filename_(date_) not in existing_files
    ]
    return missing_dates
