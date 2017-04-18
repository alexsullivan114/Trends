from datetime import timedelta
from datetime import date
from datetime import datetime
from pandas import DataFrame
from pandas import concat
import pandas as pd
import os
import itertools


def normalize(path):
    os.makedirs(os.path.join(path, "normalized"))
    normalized_path = os.path.join(path, "normalized")
    # Prep the first normalized set - which is just the first set non modified.
    df = pd.read_csv(filepath_or_buffer=os.path.join(path, '0.csv'))
    df.to_csv(path_or_buf=os.path.join(normalized_path, "0.csv"), index=False)
    final_df = DataFrame()
    try:
        for i in itertools.count():
            df1 = pd.read_csv(filepath_or_buffer=os.path.join(normalized_path, str(i) + '.csv'), index_col=False)
            df2 = pd.read_csv(filepath_or_buffer=os.path.join(path, str(i + 1) + '.csv'), index_col=False)
            normalize_pair(df1, df2)
            df1.to_csv(path_or_buf=os.path.join(normalized_path, str(i) + '.csv'), index=False)
            df2.to_csv(path_or_buf=os.path.join(normalized_path, str(i + 1) + '.csv'), index=False)
            final_df = pd.concat([final_df, df2])
            final_df = final_df.drop_duplicates(subset='date', keep='last')
    except FileNotFoundError:
        pass

    final_df.to_csv(path_or_buf=os.path.join(normalized_path, "master.csv"), index=False)


def normalize_pair(old, new):
    old.columns = ['date', 'value']
    new.columns = ['date', 'value']
    df_intersection = pd.merge(old, new, how='inner', on='date', suffixes=('_l', '_r'))
    l_values = df_intersection['value_l'].tolist()
    r_values = df_intersection['value_r'].tolist()
    ratio = sum(l_values) / sum(r_values)
    # Actually apply the normalizing ratio.
    new['value'] = new['value'].apply(lambda x: x * ratio)
    average_pair(old, new)


def average_pair(old, new):
    df_intersection = pd.merge(old, new, how='inner', on='date', suffixes=('_l', '_r'))
    intersection_date_series = df_intersection['date']
    for d in intersection_date_series:
        old_value = old.loc[old['date'] == d]['value'].values[0]
        new_value = new.loc[new['date'] == d]['value'].values[0]
        average = (old_value + new_value) / 2
        new.loc[new.date == d, 'value'] = average
