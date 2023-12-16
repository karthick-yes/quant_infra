import yfinance as yf
import pandas as pd

data = yf.Ticker(ticker='AAVAS.NS',)
print(data.history(start="2022-01-01", end="2023-1-1"))