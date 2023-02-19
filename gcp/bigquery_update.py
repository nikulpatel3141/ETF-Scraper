"""
Standalone module for storing and retrieving ETF holdings data with bigquery.

Notes: 
- Parquet format is assumed throughout for convenience. For this data filesizes are much smaller than eg CSV
and data type handling is easy (pickle could be better however that isn't suitable for long term storage since
it generally isn't stable across Python versions)
- BigQuery has a generous free tier, so I am using it for this project. It doesn't however replace a regular
DB (eg you can't set primary keys)

#FIXME: "as_of_date" should be "DATE", not "TIMESTAMP" - however pandas datetime columns are stored as INT64 in
parquet format, not INT32 which is what BigQuery expects for DATE.

See: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet#type_conversions
"""

import logging
import os
from datetime import date, datetime
from typing import Sequence, List
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from etf_scraper.storage import (
    list_files,
    holdings_filename,
    DATE_FMT,
)

DATA_URI = os.getenv("DATA_URI")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_NAME = "etf_holdings"
HOLDINGS_TABLE_NAME = "etf_holdings"

HOLDINGS_BQ_DTYPES = [
    bigquery.SchemaField("fund_ticker", "STRING"),
    bigquery.SchemaField("as_of_date", "TIMESTAMP"),  # FIXME: see above
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("isin", "STRING"),
    bigquery.SchemaField("sedol", "STRING"),
    bigquery.SchemaField("cusip", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("amount", "FLOAT"),
    bigquery.SchemaField("weight", "FLOAT"),
    bigquery.SchemaField("security_type", "STRING"),
    bigquery.SchemaField("sector", "STRING"),
    bigquery.SchemaField("market_value", "FLOAT"),
    # bigquery.SchemaField("notional_value", "FLOAT"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("exchange", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    # bigquery.SchemaField("fx_rate", "FLOAT"),
    # bigquery.SchemaField("market_currency", "STRING"),
]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(filename)16s:%(lineno)4d:%(levelname)8s] - %(message)s",
)  # FIXME: duplication

logger = logging.getLogger(__name__)


def uris_to_bigquery(
    data_uris: Sequence[str],
    table_name: str,
    dataset_name: str,
    project_id: str,
):
    """Append ETF holdings data stored as parquet files on GCS to a BigQuery table
    that already exists.

    Note: we create the table in advance to drop unnecessary columns (different providers
    provide different columns - we're only interested in a common subset).
    """
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        ignore_unknown_values=True,
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        source_format="PARQUET",
    )
    load_job = client.load_table_from_uri(
        source_uris=data_uris, destination=table_id, job_config=job_config
    )
    load_job.result()
    return load_job


def seed_holdings_table(
    table_name: str,
    dataset_name: str,
    project_id: str,
):
    """Create a table to store scraped ETF holdings. Note: this is only
    for equities to save space, we can append
    """
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    table = bigquery.Table(table_id, schema=HOLDINGS_BQ_DTYPES)
    table = client.create_table(table)  # Make an API request.
    logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


def list_existing_data(
    table_name: str,
    dataset_name: str,
    project_id: str,
    date_cutoff: date | None = None,
) -> pd.DataFrame:
    """Query for fund ticker + date pairs already in the database.
    Can also supply a minimum date to avoid returning too much data.
    """
    query = f"""
    SELECT DISTINCT fund_ticker, as_of_date
    FROM {dataset_name}.{table_name}
    """

    if date_cutoff:
        date_str = date_cutoff.strftime("%Y-%m-%d")
        query = f"""
        {query}
        WHERE as_of_date >= '{date_str}
        """

    ticker_dates = pd.read_gbq(query, project_id=project_id, dialect="standard")
    ticker_dates.loc[:, "as_of_date"] = pd.to_datetime(ticker_dates["as_of_date"])
    return ticker_dates


def list_new_uris(
    data_uri: str,
    table_name: str,
    dataset_name: str,
    project_id: str,
) -> List[str]:
    """List for holdings files in data_uri not yet in the holdings table

    Returns: a list of .parquet uris under data_uri not yet pushed to our
    table (based on the filenames).
    """
    data_uris = list_files(data_uri, ".parquet")
    existing_ticker_dates = list_existing_data(
        table_name,
        dataset_name,
        project_id,
    )
    to_filename = lambda x: holdings_filename(
        x["fund_ticker"], x["as_of_date"], ".parquet"
    )
    existing_files = existing_ticker_dates.apply(
        to_filename, axis=1
    ).to_numpy()  # less error prone than comparing raw ticker + dates
    to_push, collected_files = [], []

    for uri in data_uris:
        filename = Path(uri).name
        if filename not in existing_files:
            if filename in collected_files:
                logger.warning(f"Found duplicate holdings file {uri}, skipping")
            else:
                to_push.append(uri)
                collected_files.append(filename)

    return to_push


def update_holdings_table(
    data_uri: str,
    table_name: str,
    dataset_name: str,
    project_id: str,
):
    logger.info(f"Listing for files to upload at {data_uri}")
    new_uris = list_new_uris(data_uri, table_name, dataset_name, project_id)
    logger.info(
        f"Found {len(new_uris)} new files to push to {dataset_name}.{table_name}"
    )
    logger.info("Attempting to update the holdings table")
    uris_to_bigquery(
        data_uris=new_uris,
        table_name=table_name,
        dataset_name=dataset_name,
        project_id=project_id,
    )


def main():
    for k, v in {"PROJECT_ID": PROJECT_ID, "DATA_URI": DATA_URI}.items():
        if not v:
            # FIXME: can query GCP metadata for this
            # see https://stackoverflow.com/questions/65088076/how-to-find-the-current-project-id-of-the-deployed-python-function-in-google-clo
            raise ValueError(f"{k} env var not set, exiting")

    # FIXME: duplication from cloud_run_scraper.py
    # FIXME: enough to just pass DATA_URI, but append date to prevent accidentally
    # uploading loads of files
    data_uri = os.path.join(DATA_URI, datetime.now().strftime(DATE_FMT))

    update_holdings_table(
        data_uri=data_uri,
        table_name=HOLDINGS_TABLE_NAME,
        dataset_name=DATASET_NAME,
        project_id=PROJECT_ID,
    )
    logger.info("Done!")


if __name__ == "__main__":
    main()
