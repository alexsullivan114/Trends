from pandas import read_csv
import master_download

symbols = read_csv('keywords.csv').columns.tolist()
master_download.download(symbols)