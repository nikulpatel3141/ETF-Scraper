FROM python:3.11-slim

WORKDIR etf_scraper

COPY . .

RUN chmod -R +x scripts/ gcp/

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir .[cloud]

CMD ["python", "scripts/scrape_etf.py"]
