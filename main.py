import csv
import os
import requests
from dotenv import load_dotenv

load_dotenv()


api_key = os.getenv("API_KEY")

tickers = []
with open('tickers.csv', 'r') as file:
    reader = csv.reader(file)
    for ticker in reader:
        ticker = str(ticker).replace("'", "").replace("[", "").replace("]", "")
        tickers.append(ticker)

for ticker in tickers[:5]:
    print(ticker)
    url = f"https://api.polygon.io/v3/reference/tickers?ticker={ticker}&active=true&apiKey={api_key}"
    response = requests.get(url)
    print(response.json())
    
