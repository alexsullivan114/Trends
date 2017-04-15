from pytrends.request import TrendReq


class CustomTrend(TrendReq):

    MAX_WINDOW_SIZE = 260

    def __init__(self):
        google_username = "trends.acc.1@gmail.com"
        google_password = "3G2FaQu977BT"
        super().__init__(google_username, google_password, custom_useragent='Pytrends for daize')

    def __make_api_request(self, start_date, end_date, keywords):
        self.build_payload(kw_list=[keywords], timeframe=self.__date_string(start_date,end_date))
        return self.interest_over_time()

    def __date_string(self, start_date, end_date):
        start_time_string = start_date.strftime("%Y-%m-%d")
        end_time_string = end_date.strftime("%Y-%m-%d")
        return start_time_string + ' ' + end_time_string
