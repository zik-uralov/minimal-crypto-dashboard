import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BROKERS', 'localhost:29092').split(','),
    'client_id': 'crypto-analytics-dashboard'
}

# Topic Names
TOPICS = {
    'trades': 'crypto.trades',
    'metrics': 'crypto.metrics',
    'alerts': 'crypto.alerts'
}

# Crypto Symbols to Monitor
CRYPTO_SYMBOLS = ['BTC/USDT', 'ETH/USDT']

# Dashboard Configuration
DASHBOARD_CONFIG = {
    'update_interval': 2,  # seconds
    'window_size': 100,    # number of data points to display
    'volatility_period': 20,  # period for volatility calculation
    'min_trades_for_metrics': 3  # trades needed before emitting metrics
}
