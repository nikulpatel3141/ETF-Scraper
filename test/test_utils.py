from datetime import date

import pytest

from etf_scraper.utils import get_interval_query_dates
from etf_scraper.storage import parse_holdings_filename, list_unqueried_data

_MONTH_ENDS = [date(2023, i + 1, j) for i, j in enumerate([31, 28, 31, 28, 31, 30])]


def _day_list(year, month, day_list):
    return [date(year, month, x) for x in day_list]


@pytest.fixture
def existing_files():
    return [
        "gs://my.bkt_name/IVV_2022_12_31.csv",
        "gs://my.bkt_name/IVV_2022_12_30.csv",
        "gs://my.bkt_name/IVV_2022_12_29.csv",
        "/another/path/DPY_2022_12_31.csv",
        "/another/path/AB.C_2022_12_31.csv",
        "/another/path/AB.C_2022_12_29.csv",
    ]


@pytest.mark.parametrize(
    ["filename", "exp_ticker", "exp_date", "exp_extension"],
    [
        ["IVV_2022_12_31.csv", "IVV", date(2022, 12, 31), ".csv"],
        ["AB.C_2022_12_31.pq", "AB.C", date(2022, 12, 31), ".pq"],
    ],  # sometimes tickers have a . in their name
)
def test_parse_holdings_filename(filename, exp_ticker, exp_date, exp_extension):
    ticker, date_, ext = parse_holdings_filename(filename)

    assert ticker == exp_ticker
    assert date_ == exp_date
    assert ext == exp_extension


def test_list_unqueried_data(existing_files):
    exp_dates = _day_list(2022, 12, range(29, 32))
    exp_tickers = ["IVV", "DPY", "AB.C", "SPQ"]

    def _get_missing_list(ticker, inds):
        return [(ticker, exp_dates[x]) for x in inds]

    exp_missing = [
        *_get_missing_list("AB.C", [1]),
        *_get_missing_list("DPY", [0, 1]),
        *_get_missing_list("SPQ", [0, 1, 2]),
    ]

    act_missing = list_unqueried_data(
        existing_files=existing_files,
        expected_dates=exp_dates,
        expected_tickers=exp_tickers,
    )
    assert sorted(act_missing) == sorted(exp_missing)


@pytest.mark.parametrize(
    ["start_date", "end_date", "month_ends", "trading_days", "expected_days"],
    [
        ["2023-01-16", "2023-01-21", False, True, _day_list(2023, 1, range(17, 21))],
        ["2023-01-16", "2023-01-21", False, False, _day_list(2023, 1, range(16, 21))],
        ["2023-01-16", "2023-01-16", False, True, []],
        ["2023-01-16", "2023-01-16", False, False, _day_list(2023, 1, [16])],
        ["2023-01-01", "2023-07-21", True, False, _MONTH_ENDS],
        ["2023-01-16", "2023-07-21", True, True, _MONTH_ENDS],
        [
            "2022-11-16",
            "2023-02-21",
            True,
            True,
            [date(2022, 11, 30), date(2022, 12, 30), date(2023, 1, 31)],
        ],
        ["2023-01-16", "2023-01-21", True, True, []],
        ["2023-01-16", "2023-01-21", True, False, []],
    ],
)
def test_get_interval_query_dates(
    start_date,
    end_date,
    month_ends,
    trading_days,
    expected_days,
):
    exchange = "NYSE"
    act_dates = get_interval_query_dates(
        start_date=start_date,
        end_date=end_date,
        month_ends=month_ends,
        trading_days=trading_days,
        exchange=exchange,
    )
    assert list(act_dates) == expected_days
