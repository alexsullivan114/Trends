import pandas as pd
import os
import glob
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

StockRevenue = namedtuple("StockRevenue", ['one_week_rev', 'two_week_rev', 'four_week_rev', 'eight_week_rev', 'column',
                                           'random'])


def analyze_model(starting_cash=10000, stock_limit=3, random_check_degree=5):
    paths = [x for x in glob.glob("aggregate_stats/july_model/*")]
    df_dict = {}
    for path in paths:
        date_df = __analyze_date(pd.read_csv(path), starting_cash, stock_limit, random_check_degree)
        df_dict[os.path.basename(path).split(".csv")[0]] = date_df
    master_df = pd.concat(df_dict, axis=0)
    print(master_df.groupby(level=1).mean())


def __analyze_date(df, starting_cash, stock_limit, random_check_degree):
    # Build up a list of random values to be distilled down to one truly random stock revenue object.
    randoms = [__build_random_sample(df, starting_cash, stock_limit) for _ in range(random_check_degree)]
    random_rev = StockRevenue(sum(x.one_week_rev for x in randoms) / len(randoms),
                              sum(x.two_week_rev for x in randoms) / len(randoms),
                              sum(x.four_week_rev for x in randoms) / len(randoms),
                              sum(x.eight_week_rev for x in randoms) / len(randoms),
                              AVERAGE, "true")
    indexes = [AVERAGE, WEIGHTED_AVERAGE, IND_AVERAGE, SELF, IND_SELF]
    columns = ["one_week_vs_rand", "two_week_vs_rand", "four_week_vs_rand", "eight_week_vs_rand"]
    new_df = pd.DataFrame(index=indexes, columns=columns, dtype=float)
    # Loop through all of our revenue data and build up a dataframe representing this time period.
    # Going to define a lil' helper method here

    def percprofitstr(rev, randrev):
        if (randrev == 0 and rev == 0):
            return 1
        return rev/randrev # Bleh division by zero will ruin everything here. Fix this...

    for stockrev in __analyze_defaults(df, starting_cash, stock_limit):
        new_df.loc[stockrev.column] = [percprofitstr(stockrev.one_week_rev, random_rev.one_week_rev),
                                       percprofitstr(stockrev.two_week_rev, random_rev.two_week_rev),
                                       percprofitstr(stockrev.four_week_rev, random_rev.four_week_rev),
                                       percprofitstr(stockrev.eight_week_rev, random_rev.eight_week_rev)]
    return new_df


def __analyze_defaults(df, starting_cash, stock_limit):
    avg_rev = __calculate_profit(df, starting_cash, stock_limit, AVERAGE, ascending=False)
    weight_avg_rev = __calculate_profit(df, starting_cash, stock_limit, WEIGHTED_AVERAGE, ascending=False)
    ind_avg_rev = __calculate_profit(df, starting_cash, stock_limit, IND_AVERAGE, ascending=False)
    self_rev = __calculate_profit(df, starting_cash, stock_limit, SELF, ascending=True)
    ind_self_rev = __calculate_profit(df, starting_cash, stock_limit, IND_SELF, ascending=True)

    return [avg_rev, weight_avg_rev, ind_avg_rev, self_rev, ind_self_rev]


def __build_random_sample(df, starting_cash, stock_limit):
    return __calculate_profit(df, starting_cash, stock_limit, AVERAGE, ascending=False, random=True)


def __calculate_profit(df: pd.DataFrame, starting_cash: int, stock_limit: int, column_to_sort: str, ascending: bool,
                       random = False):
    # first sort our data frame based on the column_to_sort
    df.sort_values(column_to_sort, ascending=ascending, inplace=True)
    # now select our top stock_limit stocks to analyze
    if random:
        modified_df = df.sample(stock_limit)
    else:
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

    return StockRevenue(one_week_rev, two_week_rev, four_week_rev, eight_week_rev, column_to_sort, random)

analyze_model()