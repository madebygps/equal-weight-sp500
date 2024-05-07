import logging
import azure.functions as func
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


app = func.FunctionApp()

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

@app.schedule(schedule="0 0 20 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def UpdateDb(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')