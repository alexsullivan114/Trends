# This script loops through the _previously downloaded_
# symbols and normalizes them based off the data.
import os
import shutil
from glob import glob

from internal import trendnormalizer

symbols_path = os.path.join(os.getcwd(), "symbols")

for symbol in glob(symbols_path + "/*"):
    # Delete any existing normalization files; we're going to redo it ourselves.
    shutil.rmtree(os.path.join(symbols_path, symbol, "normalized"), ignore_errors=True)
    # Re-normalize the existing data.
    trendnormalizer.normalize(os.path.join(symbols_path, symbol))
    print("Normalized " + symbol)
