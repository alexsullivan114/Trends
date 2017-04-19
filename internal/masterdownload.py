import time
import os
from datetime import date
from datetime import datetime
from .trendnormalizer import normalize

from .slidingwindowdownload import SlidingWindowDownload

d1 = date.today()
d2 = datetime.strptime("Jan 1 2011", "%b %d %Y").date()


def download(symbols):
    for s in symbols:
        print("Fetching data for " + s)
        try:
            os.makedirs(s)
            result = SlidingWindowDownload().download(d2, d1, s)
            for i, frame in enumerate(result):
                path = os.path.join(os.getcwd(), s, str(i) + '.csv')
                frame.to_csv(path_or_buf=os.path.join(path))

            normalize(os.path.join(os.getcwd(), s))
        except KeyError:
            print("Errr got a key error...not so sure what to do here...for symbol " + s)
        time.sleep(10)
