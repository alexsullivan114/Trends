import pandas as pd
import os

PRICE_COLUMN_INDEX = 90
START_ANALYSYS_INDEX = 108
END_ANALYSIS_INDEX = 122
VALUES_START_ROW_INDEX = 2
VALUES_END_ROW_INDEX = 89


f = pd.ExcelFile("C:\\Users\\Alexs\\Downloads\\Nov100.xlsm")
sheet_names = f.sheet_names[2:]
print(sheet_names)
for s in sheet_names:
    # Parse out the sheet that we're looking at now. Exclude column headers because we're disorganized.
    df = f.parse(s, header=None)
    # Fetch the column associated with the price of the stock. We'll need this later.
    price = df[[PRICE_COLUMN_INDEX]].iloc[VALUES_START_ROW_INDEX:VALUES_END_ROW_INDEX]
    # Let's rename this column header to 'price' instead of 90 since we're not horrible people
    price.columns = ['price']
    # Great. Now let's parse out the dataframe associated withh our stock analysis.
    analysys = df.iloc[1:VALUES_END_ROW_INDEX, START_ANALYSYS_INDEX:END_ANALYSIS_INDEX]
    analysys.columns = analysys.iloc[0]
    analysys = analysys.drop(analysys.index[0])
    # Now assign our price to the analysis.
    analysys = analysys.assign(price=price['price'])
    analysys.to_csv(path_or_buf=os.path.join(os.getcwd(), "aggregate_stats", "november_model", s.lower().replace(" ", "") + ".csv"), index=False)