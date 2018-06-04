from pytrends.request import TrendReq
from datetime import timedelta
import time, random


DAYS_TO_FETCH = 260


class SlidingWindowDownload(object):

    def __init__(self):
        self.pytrend = TrendReq()

    def download(self, start_date, end_date, keywords):
        # start DAYS_TO_FETCH/2 so we can get the double data on each pass
        delta = timedelta(days=DAYS_TO_FETCH / 2)
        date = start_date - delta
        accumulated_data = []
        while date <= end_date:
            window_end = date + timedelta(days=DAYS_TO_FETCH)
            print("Requesting from " + str(date) + " to " + str(window_end))
            accumulated_data.append(self.__make_api_request(date, window_end, keywords))
            date += delta
            time.sleep(random.randint(1, 20))
        return accumulated_data

    def __make_api_request(self, start_date, end_date, keywords):
        self.pytrend.build_payload(kw_list=[keywords], timeframe=self.__date_string(start_date,end_date))
        return self.pytrend.interest_over_time()

    def __date_string(self, start_date, end_date):
        start_time_string = start_date.strftime("%Y-%m-%d")
        end_time_string = end_date.strftime("%Y-%m-%d")
        return start_time_string + ' ' + end_time_string