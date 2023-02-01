from datetime import date, datetime
import logging
from typing import Sequence

import pandas as pd
from google.cloud import bigquery

HOLDINGS_BQ_DTYPES = [
    bigquery.SchemaField("fund_ticker", "STRING"),
    bigquery.SchemaField("as_of_date", "DATE"),
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
    job_config,
):
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    load_job = client.load_table_from_uri(
        source_uris=data_uris, destination=table_id, job_config=job_config
    )
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

    return pd.read_gbq(query, project_id=project_id, dialect="standard")


# job_config = bigquery.LoadJobConfig()
