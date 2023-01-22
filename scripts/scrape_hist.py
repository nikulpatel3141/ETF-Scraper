"""Script to query for historical/latest holdings for an input set of tickers"""

from argparse import ArgumentParser, BooleanOptionalAction
import logging

from etf_scraper.main import scrape_holdings

logger = logging.getLogger(__name__)


def parse_args() -> dict:
    parser = ArgumentParser(
        prog="ETF Scraper",
        description="Script to query for historical/latest holdings for an input set of tickers",
    )
    parser.add_argument(
        "--save_dir", type=str, help="Local directory/cloud bucket to store files"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default="",
        help="YYYY-MM-DD start date for historical scraping. Leave blank to query latest holdings.",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default="",
        help="See --start_date. If blank and --start_date given, then defaults to start_date. If --start_date is blank then --end_date must be blank too.",
    )
    parser.add_argument(
        "--format",
        type=str,
        default="csv",
        choices=["csv", "parquet", "pickle"],
        help="Format to save as",
    )
    parser.add_argument("--tickers", type=str, nargs="+", help="Tickers to parse")
    parser.add_argument(
        "--overwrite",
        action=BooleanOptionalAction,
        help=(
            "If set then query all data requested, otherwise only query files missing from save_dir. "
            "Cannot be used with start + end dates (we don't know the latest holdings dates without querying)."
        ),
    )
    parser.add_argument(
        "--month_ends",
        action=BooleanOptionalAction,
        help="If set then only query for month end dates",
    )
    parser.add_argument(
        "--trading_days",
        action=BooleanOptionalAction,
        help="If set then only query for trading days. If --month_ends is also passed, then query only for trading month ends",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="NYSE",
        help="Trading calendar schedule to use if --trading_days is set",
    )
    parser.add_argument(
        "--num_threads",
        type=int,
        default=10,
        help="Number of threads to use for parallelising queries",
    )
    return vars(parser.parse_args())


if __name__ == "__main__":
    args = parse_args()
    scrape_holdings(
        tickers=args["tickers"],
        start_date=args["start_date"],
        end_date=args["end_date"],
        month_ends=args["month_ends"],
        trading_days=args["trading_days"],
        overwrite=args["overwrite"],
        save_dir=args["save_dir"],
        out_fmt=args["out_fmt"],
        num_threads=args["num_threads"],
        exchange=args["exchange"],
    )
