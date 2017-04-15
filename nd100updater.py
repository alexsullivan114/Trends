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
    last_csv = ""
    for sub_symbol in glob(symbol + "/*.csv"):
        df = pd.read_csv(sub_symbol)
        date = parse(df.tail(1)['date'].values[0])
        if date > last_date:
            last_date = date
            last_csv = sub_symbol

    time_difference = (datetime.now() - last_date).days
    d1 = last_date
    d2 = date.today()

    result = SlidingWindowDownload.SlidingWindowDownload().download(d1, d2, os.path.basename(symbol))
    print(result)


