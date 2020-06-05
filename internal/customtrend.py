import datetime
import pandas as pd
from pytrends.request import TrendReq
from internal import trendnormalizer


class CustomTrend(TrendReq):

    MAX_WINDOW_SIZE = 260

    def make_api_request(self, start_date, end_date, keywords):
        # If we have less than 7 days then we just get the last n days and average the hours out into individual days.
        delta = end_date - start_date
        if delta <= datetime.timedelta(days=7):
            print("Fetching data for " + str(keywords) + " for date range: " + self.__date_string(start_date, end_date))
            self.build_payload(kw_list=[keywords], timeframe="now 7-d")
            df = self.interest_over_time().resample('D').mean().tail(delta.days)
            return df
        date_string = self.__date_string(start_date,end_date)
        self.build_payload(kw_list=[keywords], timeframe=date_string)
        print("Fetching data for " + str(keywords) + " for date range: " + date_string)
        df = self.interest_over_time()
        thisday = datetime.date.today()# - datetime.timedelta(days=1)
        if end_date.date() == thisday:
            return self.up_to_date_data(df, keywords)
        else:
            return df

    @staticmethod
    def __date_string(start_date, end_date):
        start_time_string = start_date.strftime("%Y-%m-%d")
        end_time_string = end_date.strftime("%Y-%m-%d")
        return start_time_string + ' ' + end_time_string

    def up_to_date_data(self, df, keywords):
        self.build_payload(kw_list=[keywords], timeframe="now 7-d")
        # We get the last 7 days, which comes in the form of hour by hour data, and aggregate the hours into days.
        # Luckily pandas makes this really really easy, which is awesome.
        latest = self.interest_over_time().resample('D').mean()
        # Now we want to normalize the new portions of data against the provided data so it seems like one
        # continuous block of data. To do this we need to convert our datetime index into a normal column, since the
        # normalizer expects data in that format.
        df.reset_index(level=[0], inplace=True)
        latest.reset_index(level=[0], inplace=True)
        trendnormalizer.normalize_pair(df, latest)
        # Now let's convert back to the datetime index
        df.set_index('date', inplace=True)
        latest.set_index('date', inplace=True)
        # Now we need to just add the missing bit onto the original df and return that.
        last_date_original = df.index.max()
        last_date_new = latest.index.max()
        df_range = latest[last_date_original: last_date_new]
        return pd.concat([df, df_range])