import csv
import datetime
import os
from zoneinfo import ZoneInfo
import requests

    
import alpaca
from alpaca.data.live.stock import *
from alpaca.data.historical.stock import *
from alpaca.data.requests import *
from alpaca.data.timeframe import *
from alpaca.trading.client import *
from alpaca.trading.stream import *
from alpaca.trading.requests import *
from alpaca.trading.enums import *
from alpaca.common.exceptions import APIError
from dotenv import load_dotenv

load_dotenv()


alpaca_key = os.getenv("ALPACA_KEY")
alpaca_secret = os.getenv("ALPACA_SECRET")
data_api_url = None


# keys required for stock historical data client
client = StockHistoricalDataClient(alpaca_key, alpaca_secret)
# setup stock historical data client
stock_historical_data_client = StockHistoricalDataClient(alpaca_key, alpaca_secret, url_override = data_api_url)

tickers = []
with open('tickers.csv', 'r') as file:
    reader = csv.reader(file)
    for ticker in reader:
        ticker = str(ticker).replace("'", "").replace("[", "").replace("]", "")
        tickers.append(ticker)

now = datetime.now(ZoneInfo("America/New_York"))

for ticker in tickers[:1]:
    req = StockBarsRequest(
    symbol_or_symbols = [ticker],
    timeframe=TimeFrame(amount = 1, unit = TimeFrameUnit.Hour), # specify timeframe
    start = now - timedelta(days = 5),                          # specify start datetime, default=the beginning of the current day.
    # end_date=None,                                        # specify end datetime, default=now
    limit = 1,                                               # specify limit
    )
    print(stock_historical_data_client.get_stock_bars(req).df)