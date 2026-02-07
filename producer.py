from kafka import KafkaProducer
import json
import time
import requests
from datetime import datetime

API_KEY = "Q6BFDXXG0TA22QVY"
SYMBOLS = ["AAPL", "GOOG", "MSFT", "TSLA"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_stock_price(symbol):
    url = (
        "https://www.alphavantage.co/query"
        f"?function=GLOBAL_QUOTE&symbol={symbol}&apikey={API_KEY}"
    )
    response = requests.get(url)
    data = response.json()

    try:
        price = float(data["Global Quote"]["05. price"])
        return price
    except:
        return None

while True:
    for symbol in SYMBOLS:
        price = get_stock_price(symbol)

        if price is None:
            continue

        event = {
            "symbol": symbol,
            "price": price,
            "timestamp": datetime.utcnow().isoformat()
        }

        producer.send("stock_events", value=event)
        print("Sent:", event)

        time.sleep(2)
