from pytrends.request import TrendReq
import datetime
from internal import trendnormalizer
import pandas as pd
class CustomTrend(TrendReq):

    MAX_WINDOW_SIZE = 260

    def __init__(self):
        google_username = "trends.acc.1@gmail.com"
        google_password = "3G2FaQu977BT"
        super().__init__(google_username, google_password, custom_useragent='Pytrends for daize')

    def make_api_request(self, start_date, end_date, keywords):
        date_string = self.__date_string(start_date,end_date)
        self.build_payload(kw_list=[keywords], timeframe=date_string)
        print("Fetching data for " + str(keywords) + " for date range: " + date_string)
        df = self.interest_over_time()
        if end_date == datetime.date.today():
            return self.up_to_date_data(df, keywords)
        else:
            return df

    @staticmethod
    def __date_string(start_date, end_date):
        start_time_string = start_date.strftime("%Y-%m-%d")
        end_time_string = end_date.strftime("%Y-%m-%d")
        return start_time_string + ' ' + end_time_string

    def up_to_date_data(self, df, keywords):
        self.build_payload(kw_list=keywords, timeframe="now 7-d")
        # We get the last 7 days, which comes in the form of hour by hour data, and aggregate the hours into days.
        # Luckily pandas makes this really really easy, which is awesome.
        latest = self.interest_over_time().resample('D').mean()
        # Now we want to normalize the new portions of data against the provided data so it seems like one
        # continuous block of data
        trendnormalizer.normalize_pair(df, latest)
        # Now we need to just add the missing bit onto the original df and return that.
        last_date_original = df.index.max()
        last_date_new = latest.index.max()
        df_range = latest[last_date_original: last_date_new]
        return pd.concat(df, df_range)