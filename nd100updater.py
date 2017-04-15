import pandas as pd
import SlidingWindowDownload
from glob import glob
from dateutil.parser import  parse
from datetime import datetime
from datetime import date
import os

for symbol in glob("symbols/*"):
    # Search through the base symbol directory for our last non normalized reading.
    print(symbol)
    # For now we're just going to look for the latest date and assume that's in the last csv. If it's not everything
    # will be bad
    last_date = datetime.min
    last_csv_index = 0
    for sub_symbol in glob(symbol + "/*.csv"):
        df = pd.read_csv(sub_symbol)
        date = parse(df.tail(1)['date'].values[0])
        if date >= last_date:
            last_date = date
            last_csv_index = int(os.path.basename(sub_symbol).split(".")[0])

    if not last_date < datetime.now():
        print("Skipping symbol " + symbol + " since it already has up to date information!")
        continue

    time_difference = (datetime.now() - last_date).days
    d1 = last_date
    d2 = date.today()

    result = SlidingWindowDownload.SlidingWindowDownload().download(d1, d2, os.path.basename(symbol))
    for i, frame in enumerate(result):
        path = os.path.join(os.getcwd(), symbol, str(i + last_csv_index + 1) + '.csv')
        frame.to_csv(path_or_buf=os.path.join(path))


