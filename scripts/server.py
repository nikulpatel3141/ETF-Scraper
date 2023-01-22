"""Start a REST API for sending queries to. Intended to be deployed to the cloud (eg GCP Cloud Run)"""

from typing import List
from fastapi import FastAPI
from pydantic import BaseModel


from etf_scraper.storage import SaveFormat, format_hist_query_output
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
def scrape(scrape_request: ETFScrapeRequest) -> List:
    rpt = scrape_holdings(
        tickers=scrape_request.tickers,
        start_date=scrape_request.start_date,
        end_date=scrape_request.end_date,
        month_ends=scrape_request.month_ends,
        trading_days=scrape_request.trading_days,
        overwrite=scrape_request.overwrite,
        save_dir=scrape_request.save_dir,
        out_fmt=scrape_request.format.value,
        num_threads=scrape_request.num_threads,
        exchange=scrape_request.exchange,
    )
    output = [{"ticker": k[0], "query_date": k[1], **v} for k, v in rpt.items()]
    return output
