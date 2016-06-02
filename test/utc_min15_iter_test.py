__author__ = 'jmettu'

import unittest
from wario.lib.cmvlib import DayUtcMin15Iter
from datetime import datetime
from datetime import timedelta

class DayUtcIterTest(unittest.TestCase):
    def test_DayUtcMin15Iter(self):
        date = datetime.strptime('16-5-25', '%y-%m-%d')
        curr_time = datetime.strptime('2016-05-24 18:30:00', '%Y-%m-%d %H:%M:%S')
        for min15 in DayUtcMin15Iter(date, 'Asia/Kolkata'):
            self.assertEquals(min15, curr_time)
            curr_time += timedelta(minutes=15)


if __name__ == '__main__':
    unittest.main()
