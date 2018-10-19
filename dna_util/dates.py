from collections import namedtuple
from datetime import datetime, timedelta
from typing import Union, Iterator, Optional

# Defines begin and end dates
Window = namedtuple("Window", ["start", "end"])


def validate_date(date: Union[str, datetime, timedelta, None]) -> str:
    """ Helper function that validates and normalizes date input

    Parameters
    -----------
    date : Union[str, datetime, timedelta, None
        Date in "YYYY-MM-DD", datetime, timedelta, or None.
        If str, ValueError will be raised if not in proper format
        If datetime, input will be converted to "YYYY-MM-DD" format.
        If timedelta, input will be **added** to the current date (e.g.
        timedelta(days=-1) for yesterday's date)
        If None, date will default to yesterday's date.

    Returns
    --------
    str : date in "YYYY-MM-DD" format
    """
    if date is None:
        date = datetime.now() + timedelta(days=-1)
    elif isinstance(date, str):
        try:
            date = datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Invalid date string: {date!r}. "
                             "Must be in YYYY-MM-DD format.")
    elif isinstance(date, timedelta):
        date = datetime.now() + date
    elif not hasattr(date, 'strftime'):
        raise ValueError(f"Invalid input type {type(date)!r}. Input must be "
                         f"of type str, datetime, timedelta, or None")
    return date.strftime("%Y-%m-%d")


def str_to_datetime(date: Union[str, datetime]) -> datetime:
    """ Convert a string to a datetime object

    Parameters
    -----------
    date : Union[str, datetime]
        Date in "YYYY-MM-DD" format or a datetime object

    Returns
    --------
    datetime
    """
    if not isinstance(date, datetime):
        try:
            date = datetime.strptime(date, "%Y-%m-%d")
        except (ValueError, TypeError):
            raise ValueError("date needs to be either a datetime object or a "
                             "string in 'YYYY-MM-DD' format")
    return date


def get_window(date: Union[str, datetime], n_days: int,
               lookback: bool = True) -> Window:
    """ Returns namedtuple with start and end timestamps

    Parameters
    -----------
    date : Union[str, datetime]
        Date of reference in "YYYY-MM-DD" or datetime

    n_days : int
        Number of days in window

    lookback : bool (default True)
        If True, window.start will be n_days back from 'date'
        else, window.end will be n_days forward from 'date'

    Returns
    --------
    Window (namedtuple)

    Examples
    ---------
    >>> w = get_window(date='2018-02-05', n_days=1, lookback=True)
    >>> w.start == '2018-02-04'
    >>> w.end == '2018-02-05'

    >>> w = get_window(date='2018-02-05', n_days=1, lookback=False)
    >>> w.start == '2018-02-05'
    >>> w.end == '2018-02-06'
    """
    ds = str_to_datetime(date)
    t_delta = timedelta(days=n_days)

    if lookback:
        return Window(start=validate_date(ds - t_delta), end=date)

    return Window(start=date, end=validate_date(ds + t_delta))


def get_daterange(date_window: Optional[Window] = None,
                  start_date: Optional[Union[str, datetime]] = None,
                  end_date: Optional[Union[str, datetime]] = None, freq: str = "D") -> Iterator[str]:
    """ Return list of days between two dates in "YYYY-MM-DD" format

    Parameters
    -----------
    date_window : Window
        This argument takes priority over start_date and end_date if specified

    start_date : Union[str, datetime, None]

    end_date : Union[str, datetime, None]

    freq : str (default "D")
        The frequency that the daterange should return. The default is to
        return daily.  If a non-daily frequency is specified, pandas is used to
        generate the daterange and any frequency pandas accepts can be used.
        NOTE: If freq != 'D', pandas will be imported

    Returns
    --------
    Iterator[str]
    """
    if date_window is None or not isinstance(date_window, Window):
        if not all([start_date, end_date]):
            raise ValueError("Either a window or both a start and end date "
                             "must be specified")
        start_date = str_to_datetime(start_date)
        end_date = str_to_datetime(end_date)
    else:
        start_date = str_to_datetime(date_window.start)
        end_date = str_to_datetime(date_window.end)

    if freq == "D":
        for day_offset in range(int((end_date - start_date).days) + 1):
            yield validate_date(start_date + timedelta(days=day_offset))
    else:
        import pandas as pd
        for date in pd.date_range(start_date, end_date, freq=freq):
            yield validate_date(date)
