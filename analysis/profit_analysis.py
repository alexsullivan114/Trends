import pandas as pd
from collections import namedtuple

STOCK = "stock"
PRICE = "price"
AVERAGE = "average"
WEIGHTED_AVERAGE = "Weighted Ave"
IND_AVERAGE = "Ind average"
SELF = "Self"
IND_SELF = "Ind Self"
ONE_WEEK = "1 week"
TWO_WEEK= "2 weeks"
FOUR_WEEK = "4 weeks"
EIGHT_WEEK = "8 weeks"

StockRevenue = namedtuple("StockRevenue", ['one_week_rev', 'two_week_rev', 'four_week_rev', 'eight_week_rev', 'column'])


def analyze(df, starting_cash=100000, stock_limit=5):
    for stockrev in __analyze_defaults(df, starting_cash, stock_limit):
        print ("Evaluation data for column " + stockrev.column + " with starting cash: $" + str(starting_cash) +
               " and stock limit: " + str(stock_limit))
        print ("One week sell point: " + str(stockrev.one_week_rev))
        print ("Two weeks sell point: " + str(stockrev.two_week_rev))
        print ("Four weeks sell point: " + str(stockrev.four_week_rev))
        print ("Eight weeks sell point: " + str(stockrev.eight_week_rev))

def __analyze_defaults(df, starting_cash, stock_limit):
    avg_rev = __calculate_profit(df, starting_cash, stock_limit, AVERAGE, ascending=False)
    weight_avg_rev = __calculate_profit(df, starting_cash, stock_limit, WEIGHTED_AVERAGE, ascending=False)
    ind_avg_rev = __calculate_profit(df, starting_cash, stock_limit, IND_AVERAGE, ascending=False)
    self_rev = __calculate_profit(df, starting_cash, stock_limit, SELF, ascending=True)
    ind_self_rev = __calculate_profit(df, starting_cash, stock_limit, IND_SELF, ascending=True)
    all_stocks = __calculate_profit(df, starting_cash, 1000, AVERAGE, ascending=False)

    return [avg_rev, weight_avg_rev, ind_avg_rev, self_rev, ind_self_rev, all_stocks]


def __calculate_profit(df: pd.DataFrame, starting_cash: int, stock_limit: int, column_to_sort: str, ascending: bool):
    # first sort our data frame based on the column_to_sort
    df.sort_values(column_to_sort, ascending=ascending, inplace=True)
    # now select our top stock_limit stocks to analyze
    modified_df = df.head(stock_limit)
    one_week_rev = 0
    two_week_rev = 0
    four_week_rev = 0
    eight_week_rev = 0
    # Total available $ to spend per symbol - this weighs them equally. In the future we may want to weigh the top
    # stocks more.
    available_cash_per_symbol = (1/len(modified_df.index)) * starting_cash
    for index, row in modified_df.iterrows():
        # Pull some values out.
        price = row[PRICE]
        one_week_change = row[ONE_WEEK]
        two_week_change = row[TWO_WEEK]
        four_week_change = row[FOUR_WEEK]
        eight_week_change = row[EIGHT_WEEK]
        # See how many shares we buy based off our available cash...we're accepting fractions of shares so that the
        # amount of cash per share doesn't end up being a huge factor as stock_limit increases.
        num_shares = available_cash_per_symbol / price
        # Now let's figure out the selling price for each one/two/etc week change.
        one_week_sell_pr = price * one_week_change
        two_week_sell_pr = price * two_week_change
        four_week_sell_pr = price * four_week_change
        eight_week_sell_pr = price * eight_week_change
        # Now start figuring out our revenue...
        one_week_rev += num_shares * one_week_sell_pr
        two_week_rev += num_shares * two_week_sell_pr
        four_week_rev += num_shares * four_week_sell_pr
        eight_week_rev += num_shares * eight_week_sell_pr

    return StockRevenue(one_week_rev, two_week_rev, four_week_rev, eight_week_rev, column_to_sort)



july1 = pd.read_csv("aggregate_stats/july1.csv")
analyze(july1)