#!/usr/bin/env bash

python3 cloud_run_scraper.py

python3 bigquery_update.py

python3 calculate_flows.py