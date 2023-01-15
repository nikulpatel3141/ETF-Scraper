import sys


if sys.version_info[0] < 3:
    raise RuntimeError("Must be using Python 3")


if sys.version_info[1] >= 10:
    # see https://setuptools.pypa.io/en/latest/userguide/datafiles.html
    from importlib.resources import files
else:
    from importlib_resources import files


LISTINGS_FILE = files("etf_scraper.data").joinpath("listings.csv")
