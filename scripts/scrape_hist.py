"""Script to query for historical/latest holdings for an input set of tickers

"""

from argparse import ArgumentParser
from functools import partial

from etf_scraper import ETFScraper
from etf_scraper.storage import list_unqueried_data, query_hist_ticker_dates, save_func


def parse_args() -> dict:
    parser = ArgumentParser("")
    parser.add_argument("--save_dir", type=str)
    parser.add_argument("--start_date", type=str, default=None)
    parser.add_argument("--end_date", type=str, default=None)
    parser.add_argument("--format", type=str, default="csv")
    parser.add_argument("--tickers", type=str, nargs="+")
    parser.add_argument("--overwrite", type=bool, default=False)
    return vars(parser.parse_args())



def main():
    