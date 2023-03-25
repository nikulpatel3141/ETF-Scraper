#!/usr/bin/env bash

cd "$(dirname "$0")"

git config --global user.email "gcp@gcp.gcp"
git config --global user.name "GCP"

TMP_OUT_DIR='/tmp/etf_scraper_output'

mkdir ${TMP_OUT_DIR}
export SAVE_DIR=${TMP_OUT_DIR}/output.json

# python3 cloud_run_scraper.py

# python3 bigquery_update.py

python3 calculate_flows.py # outputs to SAVE_DIR

python3 setup_gh.py

cd TMP_OUT_DIR

git init
git remote set origin ${GH_OUT_REPO_URL}
git add .
git commit -m "Cloud Run scheduled run $(date)"
git push origin master