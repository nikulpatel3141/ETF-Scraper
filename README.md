# ETF-Scraper

![IShares](https://github.com/nikulpatel3141/ETF-Scraper/actions/workflows/check_ishares.yml/badge.svg)
![SSGA](https://github.com/nikulpatel3141/ETF-Scraper/actions/workflows/check_ssga.yml/badge.svg)
![Vanguard](https://github.com/nikulpatel3141/ETF-Scraper/actions/workflows/check_vanguard.yml/badge.svg)
![Invesco](https://github.com/nikulpatel3141/ETF-Scraper/actions/workflows/check_invesco.yml/badge.svg)

Scrape public ETF and Mutual Fund holdings information, currently from iShares, SSGA, Vanguard and Invesco.

## Installation

```bash
git clone https://github.com/nikulpatel3141/ETF-Scraper
cd ETF-Scraper/
pip install .
```

## Usage

### Basic Python Usage

```python
from etf_scraper import ETFScraper

fund_ticker = "IVV" # IShares Core S&P 500 ETF
holdings_date =  "2022-12-30" # or None to query the latest holdings

etf_scraper = ETFScraper()

holdings_df = etf_scraper.query_holdings(fund_ticker, holdings_date)
```

###

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
