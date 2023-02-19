#!/usr/bin/env python3

"""Script to run as a `Google Cloud Run Job`. Expects all parameters to be passed as env vars
since this is how Cloud Run Jobs accept parameters

To run with minimal configuration, set TICKER_FILE (one ticker per line) and SAVE_DIR (path to save output).

Note: this will save to SAVE_DIR/YYYY_MM_DD for convenience (for my use case).

Each run instance will scrape a subset of tickers in the passed file based on the CLOUD_RUN_TASK_INDEX
env var.

For more details see:
- https://cloud.google.com/run/docs/quickstarts/jobs/build-create-python
- https://cloud.google.com/sdk/gcloud/reference/beta/run/jobs/create

Usage:

```bash
NUM_TASKS=20

gcloud beta run jobs create scraper-job \
    --image gcr.io/PROJECT_ID/scraping-job \
    --tasks ${NUM_TASKS} \
    --set-env-vars TICKER_FILE='gs://path/to/ticker/file.txt',SAVE_DIR='gs://path/to/holdings/dir/' \
    --max-retries 1 \
    --region REGION
```
"""
import os
import logging
from datetime import datetime

import pandas as pd

from etf_scraper.main import scrape_holdings
from etf_scraper.utils import parse_bool_env, get_list_chunk
from etf_scraper.storage import format_hist_query_output, DATE_FMT

# save parameters
TICKER_FILE = os.getenv("TICKER_FILE")
SAVE_DIR = os.getenv("SAVE_DIR")
SAVE_FMT = os.getenv("SAVE_FMT", "parquet")

# query parameters
START_DATE = os.getenv("START_DATE", "")
END_DATE = os.getenv("END_DATE", "")
MONTH_ENDS = parse_bool_env("MONTH_ENDS")  # False by default
TRADING_DAYS = parse_bool_env("TRADING_DAYS")
OVERWRITE = parse_bool_env("OVERWRITE")
EXCHANGE = os.getenv("EXCHANGE", "NYSE")

# save logs if location given
LOG_DIR = os.getenv("LOG_DIR", None)

# task parameters
NUM_THREADS = int(os.getenv("NUM_THREADS", 10))

TASK_COUNT = int(os.getenv("CLOUD_RUN_TASK_COUNT", 1))
TASK_INDEX = int(os.getenv("CLOUD_RUN_TASK_INDEX", 0))
TASK_ATTEMPT = int(os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0))

# hardcode to parquet for convenience
_TIME_STR_NOW = datetime.now().strftime(DATE_FMT + "__%H_%M")
LOGFILE = f"etf_scraper_log_{_TIME_STR_NOW}_{TASK_INDEX}_{TASK_COUNT}.parquet"

PARQUET_SAVE_OPTS = {
    "engine": "pyarrow",
    "coerce_timestamps": "ms",
    "allow_truncated_timestamps": True,
}

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(filename)16s:%(lineno)4d:%(levelname)8s] - %(message)s",
)

logger = logging.getLogger(__name__)


def log_output(out):
    num_scraped = len([1 for k in out.values() if "error" not in k])
    logger.info(f"Scraped {num_scraped} holdings")

    num_saved = len([1 for k in out.values() if "save_path" in k and k["save_path"]])
    logger.info(f"Saved {num_saved} holdings")


def main():
    logger.info(
        f"Starting attempt {TASK_ATTEMPT} for task {TASK_INDEX} out of {TASK_COUNT}"
    )

    if not TICKER_FILE:
        raise ValueError(f"No TICKER_FILE env var set, can't retrieve tickers to query")

    tickers = pd.read_csv(TICKER_FILE, header=None).to_numpy().reshape(-1)
    tickers_to_query = get_list_chunk(tickers, TASK_INDEX, TASK_COUNT)

    if len(tickers_to_query) == 0:
        logger.info(f"No tickers to query, exiting")

    if not SAVE_DIR:
        raise ValueError(
            f"No SAVE_DIR env var set, don't know where to save the output"
        )

    if not len(tickers_to_query):
        logger.info(f"No tickers to query")
        return

    save_dir = os.path.join(SAVE_DIR, datetime.now().strftime(DATE_FMT))
    logger.info(f"Will save any scraped holdings to {save_dir}")

    out = scrape_holdings(
        tickers_to_query,
        START_DATE,
        END_DATE,
        MONTH_ENDS,
        TRADING_DAYS,
        OVERWRITE,
        save_dir,
        SAVE_FMT,
        NUM_THREADS,
        EXCHANGE,
    )
    log_output(out)

    if LOG_DIR:
        logfile_path = os.path.join(LOG_DIR, LOGFILE)
        logger.info(f"Saving scraping logs to {logfile_path}")
        format_hist_query_output(out).to_parquet(logfile_path)


if __name__ == "__main__":
    main()
