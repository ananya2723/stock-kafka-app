import json
import time
import os
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")

SYMBOLS = ["RELIANCE", "TCS", "INFY", "HDFCBANK"]

def create_producer():
    """Create Kafka producer with retry logic"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers="localhost:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks='all',
                retries=3,
                request_timeout_ms=10000
            )
            print("✅ Connected to Kafka broker successfully!")
            return producer
        except NoBrokersAvailable:
            retry_count += 1
            print(f"❌ Cannot connect to Kafka at localhost:9092")
            print(f"   Retry {retry_count}/{max_retries}")
            if retry_count < max_retries:
                time.sleep(5)
            else:
                print("\n❌ Failed to connect. Troubleshoot:")
                print("   1. Check if Docker is running: docker ps")
                print("   2. Check Kafka logs: docker logs <container_id>")
                print("   3. Verify port 9092 is exposed")
                raise

def get_stock_price(symbol):
    """Fetch stock price from API"""
    try:
        url = "https://indian-stock-exchange-api2.p.rapidapi.com/stock"
        headers = {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": RAPIDAPI_HOST
        }
        params = {"symbol": symbol}
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code != 200:
            print(f"⚠️  API Error for {symbol}: Status {response.status_code}")
            return None
        
        data = response.json()
        price = float(data.get("currentPrice"))
        return price
        
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Request error for {symbol}: {e}")
        return None
    except (KeyError, ValueError, TypeError) as e:
        print(f"⚠️  Data parsing error for {symbol}: {e}")
        return None

def main():
    """Main producer loop"""
    producer = create_producer()
    
    try:
        while True:
            for symbol in SYMBOLS:
                try:
                    price = get_stock_price(symbol)
                    
                    if price is None:
                        print(f"⏭️  Skipping {symbol} - no price data")
                        continue
                    
                    event = {
                        "symbol": symbol,
                        "price": price,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    # Send to Kafka with callback
                    future = producer.send("stock_events", value=event)
                    record_metadata = future.get(timeout=10)
                    
                    print(f"📤 PRICE SENT: {event['symbol']} = ₹{event['price']} | Partition: {record_metadata.partition}")
                    
                except KafkaError as e:
                    print(f"❌ Kafka Error: {e}")
                    break
                except Exception as e:
                    print(f"❌ Unexpected error for {symbol}: {e}")
                    continue
                
                time.sleep(10)
    
    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer closed.")

if __name__ == "__main__":
    main()