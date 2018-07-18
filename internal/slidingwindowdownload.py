from pytrends.request import TrendReq
from datetime import timedelta
import numpy as np
import pandas as pd
import time, random

from internal.utilities import normalize_date

DAYS_TO_FETCH = 260


def date_string(start_date, end_date):
    start_time_string = start_date.strftime("%Y-%m-%d")
    end_time_string = end_date.strftime("%Y-%m-%d")
    return start_time_string + ' ' + end_time_string


def pad_df(df, start_date, end_date, stock_name):
    if df.empty:
        df = pd.DataFrame()
        df["date"] = pd.date_range(start_date, end_date)
        normalize_date(df)
        df[stock_name] = df["date"].map(lambda x: 0)
        df.set_index("date", inplace=True)
    return df


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
            df = self.__make_api_request(date, window_end, keywords)
            df = pad_df(df, date, window_end, keywords)
            accumulated_data.append(df)
            date += delta
            time.sleep(random.randint(1, 20))
        return accumulated_data

    def __make_api_request(self, start_date, end_date, keywords):
        self.pytrend.build_payload(kw_list=[keywords], timeframe=date_string(start_date, end_date))
        return self.pytrend.interest_over_time()
