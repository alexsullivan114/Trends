from datetime import date
from datetime import datetime

from pandas import read_csv

from internal import masterdownload

d1 = date.today()
d2 = datetime.strptime("Jan 1 2011", "%b %d %Y").date()
nd100 = read_csv("../symbol_lists/ND100.csv")
symbols = nd100['symbol'].tolist()

masterdownload.download(symbols)
