from pandas import read_csv
from datetime import date
from datetime import datetime
from SlidingWindowDownload import SlidingWindowDownload
import os
import time
import TrendNormalizer

###Failing stocks: MDLZ, NCLH

nd100 = read_csv("nd100.csv")
symbols = nd100['symbol'].tolist()

d1 = date.today()
d2 = datetime.strptime("Jan 1 2011", "%b %d %Y").date()

for s in symbols:
    print("Fetching data for " + s)
    try:
        os.makedirs(s)
        result = SlidingWindowDownload().download(d2, d1, s)
        for i, frame in enumerate(result):
            path = os.path.join(os.getcwd(), s, str(i) + '.csv')
            frame.to_csv(path_or_buf=os.path.join(path))

        TrendNormalizer.normalize(os.path.join(os.getcwd(), s))
    except KeyError:
        print("Errr got a key error...not so sure what to do here...for symbol " + s)
    time.sleep(10)
