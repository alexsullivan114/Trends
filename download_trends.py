from pytrends.request import TrendReq
from datetime import timedelta
from datetime import date
from pandas import DataFrame
from pandas import concat
from pandas import merge
import time

DAYS_FETCH = 60
WINDOW_SIZE = 20


def download_trend_data(start_date, end_date, keywords):
    return download_trend_data_rec(start_date, end_date, keywords, DataFrame())


def download_trend_data_rec(start_date, end_date, keywords, data):
    if end_date <= start_date:
        return data
    else:
        days = DAYS_FETCH
        window = WINDOW_SIZE
        if end_date - timedelta(days=DAYS_FETCH) < start_date:
            days = (end_date - start_date).days
            if days <= window:
                window = 0

        print("Fetching data for date: " + str(end_date) + " For num days: " + str(days))
        data = concat_data(download_data(end_date, keywords, days_to_fetch=days), data)
        end_date -= timedelta(days=(days - window))
        return download_trend_data_rec(start_date, end_date, keywords, data)


def concat_data(new_data, existing_data):
    merged_frame = existing_data.join(new_data, how="inner", lsuffix="l", rsuffix="r")
    ratio = calculate_ratio(merged_frame)
    if ratio < 1:
        updater = lambda x: x * ratio
        new_data = new_data.applymap(updater)
        # remove existing data overlap from new data
        new_data = new_data.drop(merged_frame.index.values)
    elif ratio > 1:
        updater = lambda x: x * (1/ratio)
        existing_data = existing_data.applymap(updater)
        # remove new data overlap from existing data
        existing_data = existing_data.drop(merged_frame.index.values)
    return concat([new_data, existing_data])


def calculate_ratio(merged_data):
    """Return our ratio of values compared between the existing data and the newly aquired data. If we return a ratio
     greater than one than that means that our existing data had a maximum less than our newly provided data, and our
     existing data needs to be adjusted down by 1/our returned ratio. If the value is less than one than that means our
     existing data had a higher maximum, and the newly provided data needs to be adjusted down by the ratio. """
    if merged_data.empty:
        return 1

    l_value_label = merged_data.columns.values[0]
    r_value_label = merged_data.columns.values[1]

    # left is existing data, right is new data.

    l_values = merged_data[l_value_label].tolist()
    r_values = merged_data[r_value_label].tolist()

    ratios = list(map(lambda x, y: (x/y), l_values, r_values))
    return sum(ratios) / len(ratios)


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