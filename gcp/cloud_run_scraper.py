#!/usr/bin/env python3

"""Script to run as a `Google Cloud Run Job`. Expects all parameters to be passed as env vars
since this is how Cloud Run Jobs accept parameters

Scrape a subset of tickers in the passed file based on the CLOUD_RUN_TASK_INDEX env var.
See: https://cloud.google.com/run/docs/quickstarts/jobs/build-create-python

Usage:

```bash
NUM_TASKS=20

gcloud beta run jobs create job-scraper \
    --image gcr.io/PROJECT_ID/scraping-job \
    --tasks ${NUM_TASKS} \
    --set-env-vars TICKER_FILE='gs://path/to/ticker/file.txt' \
    --max-retries 1 \
    --region REGION
```
"""
import os
import logging
from typing import Sequence

import pandas as pd
import numpy as np

from etf_scraper.main import scrape_holdings

# save parameters
TICKER_FILE = os.getenv("TICKER_FILE")
SAVE_DIR = os.getenv("SAVE_DIR")
SAVE_FMT = os.getenv("SAVE_FMT", "parquet")

# query parameters
START_DATE = os.getenv("START_DATE", "")
END_DATE = os.getenv("END_DATE", "")
MONTH_ENDS = bool(os.getenv("MONTH_ENDS", False))  # True if set, otherwise False
TRADING_DAYS = bool(os.getenv("TRADING_DAYS", False))
OVERWRITE = bool(os.getenv("OVERWRITE", False))
EXCHANGE = os.getenv("EXCHANGE", "NYSE")

# task parameters
NUM_THREADS = int(os.getenv("NUM_THREADS", 10))

TASK_COUNT = int(os.getenv("CLOUD_RUN_TASK_COUNT", 1))
TASK_INDEX = int(os.getenv("CLOUD_RUN_TASK_INDEX", 0))
TASK_ATTEMPT = int(os.getenv("CLOUD_RUN_TASK_ATTEMPT", 0))


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(filename)16s:%(lineno)4d:%(levelname)8s] - %(message)s",
)

logger = logging.getLogger(__name__)


def get_ticker_block(
    ticker_file: str, task_index: int, num_tasks: int
) -> Sequence[str]:
    """Read the ticker file, split into NUM_TASKS blocks and returns the
    block at task_index. If None given then returns all tickers
    """
    tickers = pd.read_csv(ticker_file, header=None).to_numpy().reshape(-1)

    if len(tickers) == 0:
        logger.info(f"No tickers found at {ticker_file}, exiting")
        return []

    if num_tasks == 1:
        logger.info(f"Only one cloud run task so using all tickers")
        return tickers

    block_size = np.ceil(len(tickers) / int(num_tasks))
    return tickers[block_size * task_index : block_size * (task_index + 1)]


def main():
    logger.info(
        f"Starting attempt {TASK_ATTEMPT} for task {TASK_INDEX} out of {TASK_COUNT}"
    )

    if not TICKER_FILE:
        raise ValueError(f"No TICKER_FILE env var set, can't retrieve tickers to query")

    if not SAVE_DIR:
        raise ValueError(
            f"No SAVE_DIR env var set, don't know where to save the output"
        )

    tickers_to_query = get_ticker_block(TICKER_FILE, TASK_INDEX, TASK_COUNT)

    if not len(tickers_to_query):
        logger.info(f"No tickers to query")
        return

    out = scrape_holdings(
        tickers_to_query,
        START_DATE,
        END_DATE,
        MONTH_ENDS,
        TRADING_DAYS,
        OVERWRITE,
        SAVE_DIR,
        SAVE_FMT,
        NUM_THREADS,
        EXCHANGE,
    )
    num_scraped = len([1 for k in out.values() if "error" not in k])
    logger.info(f"Scraped {num_scraped} holdings")


if __name__ == "__main__":
    main()
