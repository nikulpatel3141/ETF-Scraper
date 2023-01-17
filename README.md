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

### Basic Usage

It should be straightforward to get started:

```python
from etf_scraper import ETFScraper

fund_ticker = "IVV" # IShares Core S&P 500 ETF
holdings_date =  "2022-12-30" # or None to query the latest holdings

etf_scraper = ETFScraper()

holdings_df = etf_scraper.query_holdings(fund_ticker, holdings_date)
...
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
