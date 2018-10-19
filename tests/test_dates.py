from datetime import datetime
import pytest

from dna_util import dates


class TestValidateDate(object):
    def test_validate_str(self):
        ds = dates.validate_date("2018-01-01")
        assert ds == "2018-01-01"

    def test_validate_str_err(self):
        with pytest.raises(ValueError):
            dates.validate_date("2018-01-32")

    def test_validate_datetime(self):
        dt = datetime(2018, 1, 1)
        assert dates.validate_date(dt) == "2018-01-01"


class TestStrToDatetime(object):
    def test_str_to_datetime(self):
        ds = dates.str_to_datetime("2018-01-01")
        assert ds == datetime(2018, 1, 1)

    def test_datetime_to_datetime(self):
        ds = datetime(2018, 1, 1)
        assert dates.str_to_datetime(ds) == ds

    def test_wrong_input_type(self):
        with pytest.raises(ValueError):
            dates.str_to_datetime(42)

    def test_invalid_str(self):
        with pytest.raises(ValueError):
            dates.str_to_datetime("2018-01-32")


class TestGetWindow(object):
    def test_get_forward_window(self):
        w = dates.get_window("2018-02-05", n_days=1, lookback=True)
        assert w.start == "2018-02-04"
        assert w.end == "2018-02-05"

    def test_get_back_window(self):
        w = dates.get_window("2018-02-05", n_days=1, lookback=False)
        assert w.start == "2018-02-05"
        assert w.end == "2018-02-06"


class TestDateRange(object):
    def test_get_range_with_window(self):
        w = dates.Window("2017-12-30", "2018-01-01")
        daterange = [
            '2017-12-30',
            '2017-12-31',
            '2018-01-01'
        ]
        assert list(dates.get_daterange(w)) == daterange

    def test_get_range_with_str(self):
        daterange = dates.get_daterange(
            start_date="2017-12-30",
            end_date="2018-01-01"
        )
        expected_daterange = [
            '2017-12-30',
            '2017-12-31',
            '2018-01-01'
        ]
        assert list(daterange) == expected_daterange

    def test_get_monthly_with_window(self):
        w = dates.Window("2017-12-30", "2018-02-15")
        daterange = [
            "2018-01-01",
            "2018-02-01"
        ]
        assert list(dates.get_daterange(w, freq="MS")) == daterange
