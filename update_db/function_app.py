import logging
import azure.functions as func
import csv
import datetime
import os
from zoneinfo import ZoneInfo
import inspect
    
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
from azure.core.exceptions import ResourceExistsError

from azure.storage.blob.aio import BlobServiceClient, BlobClient

import asyncio

app = func.FunctionApp()

alpaca_key = os.getenv("ALPACA_KEY")
alpaca_secret = os.getenv("ALPACA_SECRET")
data_api_url = None
now = datetime.now(ZoneInfo("America/New_York"))

# keys required for stock historical data client
client = StockHistoricalDataClient(alpaca_key, alpaca_secret)
# setup stock historical data client
stock_historical_data_client = StockHistoricalDataClient(alpaca_key, alpaca_secret, url_override = data_api_url)

# setup cosmos db client
cosmos_client = CosmosClient(url= os.getenv("COSMOS_DB_ENDPOINT"), credential=os.getenv("COSMOS_DB_KEY"))
database = cosmos_client.get_database_client(os.getenv("COSMOS_DATABASE"))

container_name = os.getenv("COSMOS_DB_CONTAINER")

container = database.get_container_client(container_name)
blob_name = os.getenv("FILE_NAME")
blob_container_name = os.getenv("BLOB_CONTAINER_NAME")
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

@app.schedule(schedule="0 0 22 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
async def UpdateDb(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
        
    # GET CSV FROM BLOB STORAGE
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=blob_name)
    download_stream = await blob_client.download_blob()
    blob_data = await download_stream.readall()

    
    blob_data = blob_data.decode('utf-8')
    blob_data = blob_data.split('\n')
    
    
    # get ticker data from alpaca
    for ticker in blob_data:
        ticker = ticker.strip().replace('\r','')
        req = StockBarsRequest(
        symbol_or_symbols = [ticker],
        timeframe=TimeFrame.Day, 
        start = now - timedelta(days = 1),                          # specify start datetime, default=the beginning of the current day.
        )
        stock_bars = stock_historical_data_client.get_stock_bars(req)
        close_price = stock_bars.data[ticker][0].close
        # add item to cosmos db
        new_stock = {
        'id': ticker,
        'ticker': ticker,
        'close_price': close_price
        }
        container.upsert_item(new_stock)
        
        print(f"{ticker}: {close_price}")
        
    print("function triggered")
        
    
    
    

    # save to cosmos db
    
    