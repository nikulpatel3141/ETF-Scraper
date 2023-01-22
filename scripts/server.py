"""Start a REST API for sending queries to. Intended to be deployed to the cloud (eg GCP Cloud Run)"""

from typing import List
from fastapi import FastAPI
from pydantic import BaseModel


from etf_scraper.storage import SaveFormat
from etf_scraper.main import scrape_holdings


class ETFScrapeRequest(BaseModel):
    tickers: List[str]
    save_dir: str
    start_date: str = ""
    end_date: str = ""
    format: SaveFormat
    overwrite: bool = False
    month_ends: bool = False
    trading_days: bool = True
    exchange: str = "NYSE"
    num_threads: int = 10


app = FastAPI()


@app.get("/")
def scrape(scrape_request: ETFScrapeRequest):
    scrape_holdings(
        tickers=scrape_request.tickers,
        start_date=scrape_request.start_date,
        end_date=scrape_request.end_date,
        month_ends=scrape_request.month_ends,
        trading_days=scrape_request.trading_days,
        overwrite=scrape_request.overwrite,
        save_dir=scrape_request.save_dir,
        out_fmt=scrape_request.format,
        num_threads=scrape_request.num_threads,
        exchange=scrape_request.exchange,
    )
