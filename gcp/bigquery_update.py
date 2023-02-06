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

from datetime import date, datetime
import logging
from typing import Sequence

import pandas as pd
from google.cloud import bigquery

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
    bigquery.SchemaField("notional_value", "FLOAT"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("exchange", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("fx_rate", "FLOAT"),
    bigquery.SchemaField("market_currency", "STRING"),
]

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
        writeDisposition="WRITE_APPEND",
        sourceFormat="PARQUET",
    )
    load_job = client.load_table_from_uri(
        source_uris=data_uris, destination=table_id, job_config=job_config
    )
    load_job.w
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
    date_cutoff: date,
) -> pd.DataFrame:
    """Query for fund ticker + date pairs already in the database.
    Can also reduce the minimum date returned to avoid returning too much
    data.
    """
    query = f"""
    SELECT DISTINCT fund_ticker, as_of_date
    FROM {dataset_name}.{table_name}
    """

    if date_cutoff:
        date_str = date_cutoff.strftime("%Y-%m-%d")
        query = f"""
        {query}
        WHERE as_of_date>='{date_str}
        """

    ticker_dates = pd.read_gbq(query, project_id=project_id, dialect="standard")
    ticker_dates.loc[:, "as_of_date"] = pd.to_datetime(ticker_dates["as_of_date"])
    return ticker_dates
