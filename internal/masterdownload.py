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
            os.makedirs("symbols/" + s)
            result = SlidingWindowDownload().download(d2, d1, s)
            for i, frame in enumerate(result):
                path = os.path.join(os.getcwd(), "symbols/" + s, str(i) + '.csv')
                frame.to_csv(path_or_buf=os.path.join(path))
            normalize(os.path.join(os.getcwd(), "symbols/" + s))
            time.sleep(10)
        except KeyError:
            print("Errr got a key error...not so sure what to do here...for symbol " + s)
        except FileExistsError:
            print(
                "Found an existing directory for " + s + ". If that download is incomplete, or you otherwise want to "
                                                         "redownload it, delete it.")
            pass
