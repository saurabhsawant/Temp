__author__ = 'jmettu'

import unittest
from datetime import datetime
from wario.lib.cmvlib import DateTime

class DataTimeTest(unittest.TestCase):
    def test_validate_weekday(self):
        weekday = datetime.strptime('16-5-30', '%y-%m-%d')
        try:
            DateTime.validate_weekday(weekday)
        except ValueError:
            self.fail("Failed to check for a weekday")

        not_weekday = datetime.strptime('16-5-31', '%y-%m-%d')
        with self.assertRaises(ValueError) as context:
            DateTime.validate_weekday(not_weekday)

        self.assertTrue('Given day, {date}, does not start on Monday'.format(date=not_weekday) in context.exception)

    def test_startday_month(self):
        firstday = datetime.strptime('16-6-1', '%y-%m-%d')
        try:
            DateTime.validate_start_of_month(firstday)
        except ValueError:
            self.fail("Failed to check for a start of month")

        not_firstday = datetime.strptime('16-5-31', '%y-%m-%d')
        with self.assertRaises(ValueError) as context:
            DateTime.validate_start_of_month(not_firstday)

        self.assertTrue('Given day, {date}, is not start of the month'.format(date=not_firstday) in context.exception)

    def test_getlastday_month(self):
        date = datetime.strptime('16-5-25', '%y-%m-%d')
        self.assertEquals(DateTime.get_last_day_of_month(date).day, 31)
        date = datetime.strptime('16-12-25', '%y-%m-%d')
        self.assertEquals(DateTime.get_last_day_of_month(date).day, 31)
        date = datetime.strptime('16-2-25', '%y-%m-%d')
        self.assertEquals(DateTime.get_last_day_of_month(date).day, 29)

    def test_utc_to_any_tz(self):
        date = datetime.strptime('2016-01-17T2345', '%Y-%m-%dT%H%M')
        rollup_day = DateTime.utc_to_any_tz(date, 'Asia/Kolkata')
        self.assertEquals(rollup_day.day, 18)

    def test_next_rounded_min15(self):
        date_time = datetime.strptime('2016-01-17T2326', '%Y-%m-%dT%H%M')
        self.assertEquals(DateTime.next_rounded_min15(date_time).minute, 30)

    def test_next_day(self):
        date_time = datetime.strptime('2016-01-17T2326', '%Y-%m-%dT%H%M')
        self.assertEquals(DateTime.next_day(date_time).day, 18)

if __name__ == '__main__':
    unittest.main()

