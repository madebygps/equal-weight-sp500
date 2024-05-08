import csv
import datetime
import os
from zoneinfo import ZoneInfo
    
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
from azure.cosmos import CosmosClient

load_dotenv()


alpaca_key = os.getenv("ALPACA_KEY")
alpaca_secret = os.getenv("ALPACA_SECRET")
data_api_url = None

# keys required for stock historical data client
client = StockHistoricalDataClient(alpaca_key, alpaca_secret)
# setup stock historical data client
stock_historical_data_client = StockHistoricalDataClient(alpaca_key, alpaca_secret, url_override = data_api_url)

# setup cosmos db client
cosmos_client = CosmosClient(url= os.getenv("COSMOS_DB_ENDPOINT"), credential=os.getenv("COSMOS_DB_KEY"))
database = cosmos_client.get_database_client(os.getenv("COSMOS_DATABASE"))
container = database.get_container_client(os.getenv("COSMOS_DB_CONTAINER"))


tickers = []
with open('tickers.csv', 'r') as file:
    reader = csv.reader(file)
    for ticker in reader:
        ticker = str(ticker).replace("'", "").replace("[", "").replace("]", "")
        tickers.append(ticker)

now = datetime.now(ZoneInfo("America/New_York"))

for ticker in tickers:
    req = StockBarsRequest(
    symbol_or_symbols = [ticker],
    timeframe=TimeFrame.Day, 
    start = now - timedelta(days = 1),                          # specify start datetime, default=the beginning of the current day.
    )
    close_price = stock_historical_data_client.get_stock_bars(req).data[ticker][0].close
    new_stock = {
        'id': ticker,
        'ticker': ticker,
        'close_price': close_price
    }
    container.upsert_item(new_stock)
    print(f"{ticker}: {close_price}")
    
    # todo
    # 1. refresh db every 24 hours.
    # create api that you provide $ and it tells you how much stock to buy of each