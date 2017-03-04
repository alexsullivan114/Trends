from pytrends.request import TrendReq
from datetime import timedelta
from datetime import date
from pandas import DataFrame
import time

DAYS_FETCH = 240
WINDOW_SIZE = 20


def download_trend_data(start_date, end_date, keywords):
    looped_date = end_date
    data = DataFrame()
    while looped_date > start_date:
        try:
            days = DAYS_FETCH
            if looped_date - timedelta(days=DAYS_FETCH) < start_date:
                days = (looped_date - start_date).days

            print("Fetching data for date: " + str(looped_date) + " For num days: " + str(days))
            data = download_data(looped_date, keywords, days_to_fetch = days).append(data, ignore_index=False)
            looped_date -= timedelta(days=days + 1)
        except ValueError:
            print ("Hit API Rate Limit. Waiting...")
            time.sleep(20)
    return data


def download_data(end_date, keywords, days_to_fetch=DAYS_FETCH):
    pytrend.build_payload(kw_list=[keywords], timeframe=consumable_date_string(end_date, days_to_fetch))
    return pytrend.interest_over_time()


def consumable_date_string(end_time, days_to_fetch):
    time_delta = timedelta(days=days_to_fetch)
    start_time = end_time - time_delta
    start_time_string = start_time.strftime("%Y-%m-%d")
    end_time_string = end_time.strftime("%Y-%m-%d")
    return start_time_string + ' ' + end_time_string

google_username = "trends.acc.1@gmail.com"
google_password = "3G2FaQu977BT"

pytrend = TrendReq(google_username, google_password, custom_useragent='My Pytrends Script')

trends = "debt"
d1 = date.today()
d2 = d1 - timedelta(days=365 * 5)

result = download_trend_data(d2, d1, trends)

print(result)