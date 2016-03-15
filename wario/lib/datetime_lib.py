from datetime import datetime, timedelta
import pytz
from pytz import timezone

# Converts date in timezone to utc
def date_to_utc(date, timezone_str):
    tz = timezone(timezone_str)
    day = datetime(date.year, date.month, date.day)
    return tz.localize(day).astimezone(pytz.utc)

# Return the next rounded min15
def next_rounded_min15(dateminute):
    round_minutes = 15
    min15 = dateminute.replace(minute=(dateminute.minute / round_minutes) * round_minutes)
    return min15 + timedelta(minutes=round_minutes)

# Return one day after date
def next_day(date):
    return datetime(date.year, date.month, date.day) + timedelta(days=1)