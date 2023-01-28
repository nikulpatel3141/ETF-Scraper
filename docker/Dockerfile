FROM python:3.11-slim

WORKDIR etf_scraper

COPY . .

RUN chmod -R +x scripts/ gcp/

RUN pip install --no-cache-dir .[cloud]

CMD ["python", "scripts/scrape_etf.py"]
