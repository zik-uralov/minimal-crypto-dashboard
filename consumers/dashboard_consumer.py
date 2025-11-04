import json
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from collections import defaultdict, deque
from config import KAFKA_CONFIG, TOPICS, DASHBOARD_CONFIG

class DashboardDataManager:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'][0],
            'group.id': 'dashboard-consumer',
            'auto.offset.reset': 'latest'
        })
        
        # Subscribe to topics
        self.consumer.subscribe([TOPICS['metrics'], TOPICS['alerts']])
        
        # Data storage for dashboard
        self.metrics_data = defaultdict(lambda: deque(maxlen=DASHBOARD_CONFIG['window_size']))
        self.recent_alerts = deque(maxlen=20)
        self.current_prices = {}
        self.last_update = None
    
    def get_latest_data(self):
        """Get the latest data for dashboard display"""
        return {
            'metrics_data': {symbol: list(data) for symbol, data in self.metrics_data.items()},
            'recent_alerts': list(self.recent_alerts),
            'current_prices': self.current_prices,
            'last_update': self.last_update
        }
    
    def start_consuming(self):
        """Start consuming metrics and alerts for dashboard"""
        print("📊 Starting dashboard data consumer...")
        
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Timeout of 1 second
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Dashboard consumer error: {msg.error()}")
                        continue
                
                # Process message based on topic
                data = json.loads(msg.value().decode('utf-8'))
                
                if msg.topic() == TOPICS['metrics']:
                    # Handle metrics data
                    symbol = data['symbol']
                    data['dashboard_timestamp'] = datetime.utcnow().isoformat()
                    self.metrics_data[symbol].append(data)
                    self.current_prices[symbol] = data['current_price']
                    self.last_update = datetime.utcnow()
                    
                elif msg.topic() == TOPICS['alerts']:
                    # Handle alert data
                    data['dashboard_timestamp'] = datetime.utcnow().isoformat()
                    self.recent_alerts.append(data)
                        
        except KeyboardInterrupt:
            print("\n🛑 Stopping dashboard consumer...")
        finally:
            self.consumer.close()

# Global instance for dashboard to access
data_manager = DashboardDataManager()