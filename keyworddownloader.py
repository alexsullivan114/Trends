from pandas import read_csv

from internal import masterdownload
symbols = read_csv('../symbol_lists/keywords.csv').columns.tolist()
masterdownload.download(symbols)