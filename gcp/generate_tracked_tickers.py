"""Generate a subset of ETF tickers to track. For convenience outputs to a file subset_listings.parquet wherever
the script is called from


By default we subset on the top 100 US Equity ETFs by market cap.

Note: it would be better to subset on top ETFs by traded value since we are tracking
flows, but this is (slightly) harder to retrieve. Market cap should be a good proxy anyway.
"""
import logging
from tempfile import TemporaryDirectory
from pathlib import Path

import pandas as pd

from etf_scraper import query_listings
from etf_scraper.main import scrape_holdings

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(filename)16s:%(lineno)4d:%(levelname)8s] - %(message)s",
)

logger = logging.getLogger(__name__)


def subset_ishares(ishares_listings) -> pd.DataFrame:
    return ishares_listings[
        (ishares_listings["asset_class"] == "Equity")
        & (ishares_listings["fund_type"] == "ETF")
        & ishares_listings["region"].isin(["North America", "Global"])
    ]


def subset_ssga(ssga_listings) -> pd.DataFrame:
    return ssga_listings[
        (ssga_listings["fund_type"] == "ETF")
        & ssga_listings["region"].isin(["US"])
        & (ssga_listings["asset_class"] == "Equity")
    ]


def subset_invesco(invesco_listings) -> pd.DataFrame:
    """#FIXME: find a better way to gather this information, this
    is too error prone/data intensive for what it's doing.
    """
    invesco_tickers = invesco_listings["ticker"].to_numpy()

    with TemporaryDirectory() as td:
        scrape_holdings(invesco_tickers, "", "", False, False, False, td, "parquet")
        holdings_dfs = {
            k.stem.split("_")[0]: pd.read_parquet(k) for k in Path(td).glob("*.parquet")
        }

    eq_funds = [k for k, df in holdings_dfs.items() if "rating" not in df]
    mkt_values = {k: holdings_dfs[k]["market_value"].sum() for k in eq_funds}
    mkt_values_ = pd.Series(mkt_values).sort_values(ascending=False)

    invesco_listings_ = invesco_listings.copy()
    mkt_vals = invesco_listings_["ticker"].apply(lambda x: mkt_values_.get(x))
    invesco_listings_.loc[:, "net_assets"] = mkt_vals
    return invesco_listings_.dropna(subset=["net_assets"])


def subset_equity_etfs() -> pd.DataFrame:
    """Returns a susbet of ETFs to track - US Equity ETFs from iShares, SSGA and Invesco"""
    listings = query_listings()
    listings_dict = dict(list(listings.groupby("provider")))
    subset_funcs = {
        "IShares": subset_ishares,
        "SSGA": subset_ssga,
        "Invesco": subset_invesco,
    }

    subset_listings = pd.concat(
        (func(listings_dict[k]) for k, func in subset_funcs.items())
    )
    return subset_listings.sort_values(by="net_assets", ascending=False)


if __name__ == "__main__":
    logging.info(f"Finding all US Equity ETFs + market cap")
    subset_listings = subset_equity_etfs()[["ticker", "net_assets", "provider"]]
    save_path = Path(".").joinpath("subset_listings.parquet")
    logging.info(f"Saving to {str(save_path)}")
    subset_listings.to_parquet(save_path)
