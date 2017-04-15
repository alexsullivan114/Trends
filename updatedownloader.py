from customtrend import CustomTrend
from datetime import date


class UpdateDownloader(object):

    def __init__(self):
        self.pytrend = CustomTrend()

    def update(self, start_date, symbol):
        if start_date < date.today() - CustomTrend.MAX_WINDOW_SIZE:
            raise Exception("Can't call updater with a start date greater than " + str(CustomTrend.MAX_WINDOW_SIZE) + " days before today.")
