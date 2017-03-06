from datetime import timedelta
from datetime import date
from trend_downloader import TrendDownloader

trends = "debt"
d1 = date.today()
d2 = d1 - timedelta(days=365 * 5)

result = TrendDownloader().download_trend_data(d2, d1, trends)

print(result)