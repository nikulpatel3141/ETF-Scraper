# ETF-Scraper

![IShares](https://github.com/nikulpatel3141/ETF-Scraper/actions/workflows/check_ishares.yml/badge.svg)
![SSGA](https://github.com/nikulpatel3141/ETF-Scraper/actions/workflows/check_ssga.yml/badge.svg)
![Vanguard](https://github.com/nikulpatel3141/ETF-Scraper/actions/workflows/check_vanguard.yml/badge.svg)
![Invesco](https://github.com/nikulpatel3141/ETF-Scraper/actions/workflows/check_invesco.yml/badge.svg)

Scrape public ETF and Mutual Fund holdings information, currently from iShares, SSGA, Vanguard and Invesco.

The aim of this library is to provide a simple, consistent interface for scraping ETF/MF holdings data across multiple providers and asset classes wherever this data is available.

This is still a work in progress and may not work for _every_ listed fund. In particular, I've focused on making this work for equity ETFs, although with some more work it can be extended to work for mutual funds and credit funds (pull requests are welcome).

## Installation

```bash
git clone https://github.com/nikulpatel3141/ETF-Scraper
cd ETF-Scraper/
pip install .
```

## Usage

### Python

```python
from etf_scraper import ETFScraper

fund_ticker = "IVV" # IShares Core S&P 500 ETF
holdings_date =  "2022-12-30" # or None to query the latest holdings

etf_scraper = ETFScraper()

holdings_df = etf_scraper.query_holdings(fund_ticker, holdings_date)
```

### Command Line

The script `scripts/scrape_hist.py` allows you to scrape historical/latest fund holdings. See the parameters and examples below:

#### Scrape Monthly Historical ETF holdings

Scrape for IVV (iShares S&P500 ETF) and IVW (iShares Russell 3000 ETF) month end holdings from 2010 and save to `/tmp/out` as csv files.

```bash
mkdir -p /tmp/out # make sure the directory exists

python3 scripts/scrape_hist.py \
  --start_date 2019-01-01 --end_date 2023-01-23 \
  --tickers IVV IVW \
  --save_dir /tmp/out \
  --format csv \
  --month_ends \
  --trading_days \
  --exchange NYSE # this is the default anyway

ls /tmp/out # IVW_2022_12_30.csv IVW_2022_11_30.csv ...

```

Note: here we need to query for trading day month ends since iShares doesn't report holdings on non-trading days. This is different for Vanguard which report holdings using calendar end dates.

#### Scrape Daily Historical ETF holdings

The script will scrape daily dates in the given range by default, so remove the `--month_ends` flag (or pass `--no_month_ends`).

As of writing this is only available for iShares ETFs.

#### Scrape Latest ETF holdings

Scrape for latest SPY (SSGA S&P500 ETF) and XLF (Financial Select Sector SPDR Fund) holdings save to `/tmp/out` as parquet files.

```bash
mkdir -p /tmp/out

python3 scripts/scrape_hist.py \
  --tickers SPY XLF \
  --save_dir /tmp/out \
  --format parquet

ls /tmp/out # SPY_2023_01_20.parquet XLF_2023_01_20.parquet
```

Note: here we don't pass start/end dates or an exchange since we are retrieving the latest holdings.

#### Parameters

```
usage: scrape_hist.py [-h] --save_dir SAVE_DIR [--start_date START_DATE] [--end_date END_DATE] [--format {csv,parquet,pickle}] --tickers TICKERS [TICKERS ...] [--overwrite | --no-overwrite] [--month_ends | --no-month_ends] [--trading_days | --no-trading_days] [--exchange EXCHANGE] [--num_threads NUM_THREADS] [--log_file LOG_FILE]

Script to query for historical/latest holdings for an input set of tickers

options:
  -h, --help            show this help message and exit
  --save_dir SAVE_DIR   Local directory/cloud bucket to store files
  --start_date START_DATE
                        YYYY-MM-DD start date for historical scraping. Leave blank to query latest holdings.
  --end_date END_DATE   See --start_date. If blank and --start_date given, then defaults to start_date. If --start_date is blank then --end_date must be blank too.
  --format {csv,parquet,pickle}
                        Format to save as
  --tickers TICKERS [TICKERS ...]
                        Tickers to parse
  --overwrite, --no-overwrite
                        If set then query all data requested, otherwise only query files missing from save_dir. Cannot be used with start + end dates (we don't know the latest holdings dates without querying).
  --month_ends, --no-month_ends
                        If set then only query for month end dates
  --trading_days, --no-trading_days
                        If set then only query for trading days. If --month_ends is also passed, then query only for trading month ends
  --exchange EXCHANGE   Trading calendar schedule to use if --trading_days is set
  --num_threads NUM_THREADS
                        Number of threads to use for parallelising queries
  --log_file LOG_FILE   Optional path to output error logs
```

### Data Availability

This library queries ETF provider websites for holdings data.

We are therefore limited by what they publish, in particular:

- iShares:
  - long history available for month end data (eg IVV returns month end data from 2010)
  - recent daily history is also available
- SSGA: only latest holdings
- Invesco: only latest holdings
- Vanguard: only recent month end holdings

Also there are other ETF providers which I haven't implemented. Any pull requests to implement others are welcome!

### Available ETFs

This library can also scrape current ETF listings data from each provider:

```python
from etf_scraper import query_listings, Provider, ETFScraper

providers = [Provider.IShares] # or [] to scrape all listings
current_listings = query_listings(providers)

etf_scraper = ETFScraper(current_listings)
```

I'd expect this list to remain relatively static, so a copy is stored in the package itself in [`src/etf_scraper/data/listings.csv`](https://github.com/nikulpatel3141/ETF-Scraper/blob/5116a28697588f566693ca880605c4f68dac14c0/src/etf_scraper/data/listings.csv).

This copy is loaded by default in `ETFScraper.__init__` using `etf_scraper.load_listings`

## Status Badges

The status badges at the top of the page are from worklows running daily to query latest holdings from all currently implemented providers for their respectively most popular ETFs.

## Testing

There are some tests implemented in `src/test` to see if we correctly parse providers responses for holdings. This is definitely an area of improvement, however I'm currently not sure how often providers will change their APIs for retrieving data. Every time they do that we'll likely have to rewrite the tests.

## Related Projects

For scraping iShares ETFs + AWS integration: https://github.com/talsan/ishares

## Disclaimer

I do not take any responsibility for any (mis)use of this library and do not intend to infringe any fund provider's terms and conditions.
