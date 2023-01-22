"""Script to query for historical/latest holdings for an input set of tickers"""

from argparse import ArgumentParser, BooleanOptionalAction
from functools import partial
import logging

from etf_scraper import ETFScraper
from etf_scraper.storage import (
    list_files,
    list_unqueried_data,
    query_hist_ticker_dates,
    save_func,
    format_hist_query_output,
)
from etf_scraper.utils import get_interval_query_dates


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


def main():
    args = parse_args()

    if not args["start_date"]:
        if args["end_date"]:
            raise ValueError(
                f"start_date missing, but end_date {args['end_date']} given"
            )
        elif args["overwrite"]:
            raise ValueError(
                f"Cannot use --overwrite without --start_date (we don't know what to overwrite)"
            )

        query_dates = [None]
    else:
        if not args["end_date"]:
            logger.warning(
                f"Only start date {args['start_date']} given but not an end date, so setting end_date={args['start_date']}"
            )
            end_date = args["start_date"]
        else:
            end_date = args["end_date"]

        query_dates = get_interval_query_dates(
            args["start_date"],
            end_date,
            args["month_ends"],
            args["trading_days"],
            args["exchange"],
        )

    if not query_dates:
        params = ["start_date", "end_date", "month_ends", "trading_days"]
        if args["trading_days"]:
            params.append("exchange")
        log_str = "\n".join([f"{k}: {args[k]}" for k in params])
        logger.warning("There are no dates to query with parameters:\n " f"{log_str}")
        return

    if args["overwrite"]:
        existing_files = []
    else:
        existing_files = list_files(args["save_dir"], "." + args["format"])

    to_query = list_unqueried_data(
        existing_files,
        query_dates,
        args["tickers"],
    )
    save_func_ = partial(save_func, out_dir=args["save_dir"], out_fmt=args["format"])

    query_rpt = query_hist_ticker_dates(
        query_ticker_dates=to_query,
        etf_scraper=ETFScraper(),
        save_func=save_func_,
        num_threads=args["num_threads"],
    )

    query_rpt_fmt = format_hist_query_output(query_rpt)


if __name__ == "__main__":
    main()
