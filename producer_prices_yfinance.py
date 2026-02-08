import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import yfinance as yf

SYMBOLS = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS"]

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
                print("   2. Check Kafka logs: docker logs kafka")
                print("   3. Verify port 9092 is exposed")
                raise

def get_stock_price(symbol):
    """Fetch stock price from yfinance (free API)"""
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period='1d')
        
        if data.empty:
            print(f"⚠️  No data found for {symbol}")
            return None
        
        price = float(data['Close'].iloc[-1])
        return price
        
    except Exception as e:
        print(f"⚠️  Error fetching {symbol}: {e}")
        return None

def main():
    """Main producer loop"""
    producer = create_producer()
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            print(f"\n📍 Cycle {cycle_count} - Fetching stock prices...")
            
            for symbol in SYMBOLS:
                try:
                    price = get_stock_price(symbol)
                    
                    if price is None:
                        print(f"⏭️  Skipping {symbol} - no price data")
                        time.sleep(2)
                        continue
                    
                    # Extract symbol name for display
                    display_symbol = symbol.split('.')[0]
                    
                    event = {
                        "symbol": display_symbol,
                        "price": price,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    # Send to Kafka with callback
                    future = producer.send("stock_events", value=event)
                    record_metadata = future.get(timeout=10)
                    
                    print(f"📤 PRICE SENT: {event['symbol']} = ₹{event['price']:.2f} | Partition: {record_metadata.partition}")
                    
                except KafkaError as e:
                    print(f"❌ Kafka Error: {e}")
                    break
                except Exception as e:
                    print(f"❌ Unexpected error for {symbol}: {e}")
                    continue
                
                time.sleep(2)
            
            # Delay before next cycle
            print("⏳ Waiting 60 seconds before next cycle...")
            time.sleep(60)
    
    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer closed.")

if __name__ == "__main__":
    main()