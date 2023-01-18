FROM python:3.11

WORKDIR etf_scraper

COPY . .

RUN ls

RUN pip install -e .


CMD ['python', 'scripts/scrape_etf.py']