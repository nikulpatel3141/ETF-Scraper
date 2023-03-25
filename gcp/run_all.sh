#!/usr/bin/env bash

cd "$(dirname "$0")"

python3 cloud_run_scraper.py

python3 bigquery_update.py

python3 calculate_flows.py # outputs to SAVE_DIR
