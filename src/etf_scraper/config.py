import sys

if sys.version_info[0] < 3:
    raise RuntimeError("Must be using Python 3")


try:
    # see https://setuptools.pypa.io/en/latest/userguide/datafiles.html
    from importlib.resources import files

    LISTINGS_FILE = files("etf_scraper.data").joinpath("listings.csv")
except:
    from pathlib import Path

    LISTINGS_FILE = Path(__file__).parent.joinpath("data/listings.csv")
