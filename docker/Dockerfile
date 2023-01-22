FROM python:3.11-slim

WORKDIR etf_scraper

COPY . .

RUN pip install .

CMD ['python', 'scripts/scrape_etf.py']