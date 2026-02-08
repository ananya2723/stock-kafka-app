import sqlite3
import statistics
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_PATH = "stock_prices.db"

def get_stock_stats(symbol, hours=1):
    """Get statistics for a stock in the last N hours"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Calculate time threshold
        time_threshold = datetime.now() - timedelta(hours=hours)
        
        cursor.execute('''
            SELECT price FROM stock_prices
            WHERE symbol = ? AND received_at > ?
            ORDER BY received_at
        ''', (symbol, time_threshold.isoformat()))
        
        prices = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not prices:
            return None
        
        return {
            'symbol': symbol,
            'count': len(prices),
            'current': prices[-1],
            'min': min(prices),
            'max': max(prices),
            'avg': statistics.mean(prices),
            'std_dev': statistics.stdev(prices) if len(prices) > 1 else 0
        }
    except Exception as e:
        logger.error(f"❌ Error calculating stats: {e}")
        return None

def get_all_stocks():
    """Get all unique stocks in database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT DISTINCT symbol FROM stock_prices')
        stocks = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return stocks
    except Exception as e:
        logger.error(f"❌ Error fetching stocks: {e}")
        return []

def get_price_history(symbol, limit=10):
    """Get price history for a stock"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT price, received_at FROM stock_prices
            WHERE symbol = ?
            ORDER BY received_at DESC
            LIMIT ?
        ''', (symbol, limit))
        
        history = cursor.fetchall()
        conn.close()
        
        return history
    except Exception as e:
        logger.error(f"❌ Error fetching history: {e}")
        return []

def display_dashboard():
    """Display analytics dashboard"""
    logger.info("\n" + "="*70)
    logger.info("📊 STOCK PRICE ANALYTICS DASHBOARD")
    logger.info("="*70 + "\n")
    
    stocks = get_all_stocks()
    
    if not stocks:
        logger.warning("⚠️  No stock data available yet. Run consumer first.")
        return
    
    # Display summary statistics
    logger.info("📈 SUMMARY STATISTICS (Last 1 hour):\n")
    
    for symbol in sorted(stocks):
        stats = get_stock_stats(symbol, hours=1)
        
        if stats:
            logger.info(f"🏷️  {stats['symbol']}")
            logger.info(f"   Current Price: ₹{stats['current']:.2f}")
            logger.info(f"   Min Price:     ₹{stats['min']:.2f}")
            logger.info(f"   Max Price:     ₹{stats['max']:.2f}")
            logger.info(f"   Avg Price:     ₹{stats['avg']:.2f}")
            logger.info(f"   Std Deviation: ₹{stats['std_dev']:.2f}")
            logger.info(f"   Data Points:   {stats['count']}\n")
    
    # Display price history
    logger.info("📜 RECENT PRICE HISTORY (Last 5 prices):\n")
    
    for symbol in sorted(stocks):
        history = get_price_history(symbol, limit=5)
        
        if history:
            logger.info(f"🏷️  {symbol}")
            for i, (price, timestamp) in enumerate(history, 1):
                logger.info(f"   {i}. ₹{price:.2f} - {timestamp}")
            logger.info("")
    
    logger.info("="*70)

if __name__ == "__main__":
    display_dashboard()
