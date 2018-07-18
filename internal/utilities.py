def normalize_date(df):
    df['date'] = df['date'].dt.strftime("%Y-%m-%d")