import json
import time
from datetime import datetime, timezone
from collections import defaultdict, deque

from confluent_kafka import Consumer, Producer, KafkaError

from config import KAFKA_CONFIG, TOPICS, CRYPTO_SYMBOLS, DASHBOARD_CONFIG


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

class MetricsCalculator:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'][0],
            'group.id': 'metrics-calculator',
            'auto.offset.reset': 'latest'
        })
        
        self.producer = Producer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'][0],
            'client.id': 'metrics-producer'
        })
        
        # Subscribe to topics
        self.consumer.subscribe([TOPICS['trades']])
        
        # Data storage for calculations
        self.trade_history = {symbol: deque(maxlen=100) for symbol in CRYPTO_SYMBOLS}
        self.metrics_history = {symbol: deque(maxlen=50) for symbol in CRYPTO_SYMBOLS}
        
        # Real-time metrics
        self.current_metrics = {}
    
    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f'❌ Metrics delivery failed: {err}')
    
    def calculate_metrics(self, symbol, trades):
        """Calculate various metrics for a symbol without pandas"""
        if len(trades) < 2:
            return None
        
        # Extract prices and sort by timestamp
        sorted_trades = sorted(trades, key=lambda x: x['timestamp'])
        prices = [trade['price'] for trade in sorted_trades]
        volumes = [trade['volume'] for trade in sorted_trades]
        sizes = [trade['size'] for trade in sorted_trades]
        sides = [trade['side'] for trade in sorted_trades]
        
        current_price = prices[-1]
        previous_price = prices[0]
        
        # Basic metrics
        price_change = current_price - previous_price
        price_change_pct = (price_change / previous_price) * 100
        
        # Volume metrics
        total_volume = sum(volumes)
        avg_trade_size = sum(sizes) / len(sizes)
        
        # Volatility (standard deviation of returns)
        returns = []
        for i in range(1, len(prices)):
            returns.append((prices[i] - prices[i-1]) / prices[i-1])
        
        if returns:
            avg_return = sum(returns) / len(returns)
            variance = sum((x - avg_return) ** 2 for x in returns) / len(returns)
            volatility = (variance ** 0.5) * 100
        else:
            volatility = 0
        
        # High/Low for the period
        period_high = max(prices)
        period_low = min(prices)
        
        # Buy/Sell ratio
        buy_count = sum(1 for side in sides if side == 'buy')
        sell_count = sum(1 for side in sides if side == 'sell')
        buy_sell_ratio = buy_count / (buy_count + sell_count) if (buy_count + sell_count) > 0 else 0.5
        
        metrics = {
            'symbol': symbol,
            'timestamp': utc_now_iso(),
            'current_price': round(current_price, 4),
            'price_change': round(price_change, 4),
            'price_change_pct': round(price_change_pct, 2),
            'total_volume': round(total_volume, 2),
            'avg_trade_size': round(avg_trade_size, 4),
            'volatility': round(volatility, 2),
            'period_high': round(period_high, 4),
            'period_low': round(period_low, 4),
            'buy_sell_ratio': round(buy_sell_ratio, 2),
            'trade_count': len(trades)
        }
        
        return metrics
    
    def detect_anomalies(self, symbol, metrics):
        """Detect potential trading anomalies"""
        alerts = []
        
        # High volatility alert
        if metrics['volatility'] > 5.0:  # 5% volatility threshold
            alerts.append({
                'type': 'high_volatility',
                'symbol': symbol,
                'value': metrics['volatility'],
                'threshold': 5.0,
                'timestamp': metrics['timestamp'],
                'message': f"High volatility detected: {metrics['volatility']}%"
            })
        
        # Large price movement alert
        if abs(metrics['price_change_pct']) > 3.0:  # 3% price change
            direction = "up" if metrics['price_change_pct'] > 0 else "down"
            alerts.append({
                'type': 'large_price_move',
                'symbol': symbol,
                'value': metrics['price_change_pct'],
                'threshold': 3.0,
                'timestamp': metrics['timestamp'],
                'message': f"Large price movement {direction}: {metrics['price_change_pct']}%"
            })
        
        # High volume alert
        if metrics['total_volume'] > 1000000:  # $1M volume threshold
            alerts.append({
                'type': 'high_volume',
                'symbol': symbol,
                'value': metrics['total_volume'],
                'threshold': 1000000,
                'timestamp': metrics['timestamp'],
                'message': f"High volume: ${metrics['total_volume']:,.2f}"
            })
        
        return alerts
    
    def start_calculating(self):
        """Start consuming trades and calculating metrics"""
        print("🧮 Starting metrics calculator...")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Timeout of 1 second
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Consumer error: {msg.error()}")
                        continue
                
                # Process message
                trade = json.loads(msg.value().decode('utf-8'))
                symbol = trade['symbol']
                
                # Add trade to history
                self.trade_history[symbol].append(trade)
                
                # Calculate metrics if we have enough data
                min_trades = DASHBOARD_CONFIG.get('min_trades_for_metrics', 10)
                if len(self.trade_history[symbol]) >= min_trades:
                    metrics = self.calculate_metrics(
                        symbol, 
                        list(self.trade_history[symbol])
                    )
                    
                    if metrics:
                        # Store metrics
                        self.metrics_history[symbol].append(metrics)
                        self.current_metrics[symbol] = metrics
                        
                        # Send metrics to Kafka
                        self.producer.produce(
                            TOPICS['metrics'],
                            value=json.dumps(metrics),
                            callback=self.delivery_report
                        )
                        # Check for anomalies
                        alerts = self.detect_anomalies(symbol, metrics)
                        for alert in alerts:
                            self.producer.produce(
                                TOPICS['alerts'],
                                value=json.dumps(alert),
                                callback=self.delivery_report
                            )
                
                # Flush producer to ensure delivery
                self.producer.flush()
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping metrics calculator...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    calculator = MetricsCalculator()
    calculator.start_calculating()
