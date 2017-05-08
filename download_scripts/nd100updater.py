import os
import pandas as pd
from datetime import datetime
from datetime import timedelta
from glob import glob
from internal import trendnormalizer
from dateutil.parser import parse

from internal.customtrend import CustomTrend

SYMBOLS_LOCATION = "../symbols"


def __last_index(symbol):
    values = map(lambda x: int(os.path.basename(x).split('.csv')[0]), glob(os.path.join(SYMBOLS_LOCATION, symbol, "*.csv")))
    return max(values)


def __update(start_date, end_date, symbol, master):
    custom_trend = CustomTrend()
    df = custom_trend.make_api_request(start_date, end_date, os.path.basename(symbol))
    # Sweet save this off to preserve data.
    path = os.path.join(SYMBOLS_LOCATION, symbol, str(__last_index(symbol) + 1) + ".csv")
    df.to_csv(path_or_buf=path)
    # This is kind of frustrating, but pandas uses the date as an index and we want to convert it into a column.
    df.reset_index(level=0, inplace=True)
    # Also frustratingly when we do the above we get a date of format y-m-d-hh-mm-ss and we just want the y-m-d part.
    df['date'] = df['date'].dt.strftime("%Y-%m-%d")
    # Now we can actually normalize our data.
    trendnormalizer.normalize_pair(master, df)
    master = pd.concat([master, df])
    master = master.drop_duplicates(subset='date', keep='last')
    return master

for symbol in glob(SYMBOLS_LOCATION + "/*"):
    print("Updating symbol " + symbol)
    # Fetch our master.csv. This was created when we ran the nd100 batch downloader.
    master_normalized_path = os.path.join(symbol, "normalized", "master.csv")
    # Bail if it doesn't exist because we're fucked and god is dead
    if not os.path.isfile(master_normalized_path):
        raise FileNotFoundError("Couldn't find master.csv. You need to run the nd100 batch downloader " +
                                "for the updater to work.")

    master_normalized_df = pd.read_csv(master_normalized_path)
    # Get the last item in our data frame, get the date value, and just gimme the actual value not the god damned series
    last_date_str = master_normalized_df.tail(1)['date'].values[0]
    # Turn that shit into a datetime
    last_value_date = parse(last_date_str)
    today = datetime.today()
    if last_value_date >= today:
        raise Exception("Can't update values when the last recorded value is today or in the future")
    if (today - last_value_date).days >= CustomTrend.MAX_WINDOW_SIZE:
        raise Exception("This script is bad and can't handle updates for for more than " +
                        str(CustomTrend.MAX_WINDOW_SIZE) + " days")
    # So what we want to do here is fetch MAX_WINDOW_SIZE days, knowing *at least* one day will be overlap with our
    # corrected data. then we'll normalize our newly fetched data using that overlap, and we'll do the average thing with
    # that overlap. Then the only problem is we'll have some subset of days that don't have an overlap and we need to
    # fetch those again to get our averaging for each day. So what we'll do after is we'll just fetch the amount of
    # days we don't have values for (so some number smaller than MAX_WINDOW_SIZE), normalize it again against our
    # existing normalized data, and then average it so we have averages for everything.
    end_date = today
    start_date = today - timedelta(days=CustomTrend.MAX_WINDOW_SIZE)
    master_normalized_df = __update(start_date, end_date, os.path.basename(symbol), master_normalized_df)
    # Alright we got the first pass, now just do the (originally) missing part.
    master_normalized_df = __update(last_value_date, end_date, os.path.basename(symbol), master_normalized_df)
    # Sweet, reset the index to be our date and save it.
    master_normalized_df.set_index('date', inplace=True)
    master_normalized_df.to_csv(path_or_buf=master_normalized_path)
