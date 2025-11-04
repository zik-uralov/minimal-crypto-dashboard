import json
import time
import random
from datetime import datetime
from typing import Optional

import requests
from confluent_kafka import Producer

from config import KAFKA_CONFIG, TOPICS, CRYPTO_SYMBOLS

class CryptoDataGenerator:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'][0],
            'client.id': 'crypto-data-generator'
        })

        # Map of symbols that have a live price feed
        self.live_symbol_map = {
            'BTC/USDT': 'bitcoin',
            'ETH/USDT': 'ethereum',
        }
        self._live_reverse_map = {v: k for k, v in self.live_symbol_map.items()}

        # Seed prices fall back to reasonable defaults until the first API update
        self.current_prices = {
            'BTC/USDT': 45000.0,
            'ETH/USDT': 3000.0,
        }

        self.volume_tracker = {symbol: 0 for symbol in CRYPTO_SYMBOLS}
        self._last_live_error_log = 0.0
        self._last_live_update = 0.0
    
    def delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f'❌ Message delivery failed: {err}')
    
    def fetch_live_prices(self) -> None:
        """Refresh live prices from CoinGecko for supported symbols."""
        if not self.live_symbol_map:
            return

        # Throttle outbound requests to avoid hammering the public API.
        if time.time() - self._last_live_update < 10:
            return

        url = "https://api.coingecko.com/api/v3/simple/price"
        ids = ",".join(self.live_symbol_map.values())

        try:
            response = requests.get(
                url,
                params={'ids': ids, 'vs_currencies': 'usd'},
                timeout=5,
            )
            response.raise_for_status()
            payload = response.json()

            for market_symbol, price_payload in payload.items():
                price = price_payload.get('usd')
                if market_symbol in self._live_reverse_map and price is not None:
                    symbol = self._live_reverse_map[market_symbol]
                    self.current_prices[symbol] = float(price)
                    self._last_live_update = time.time()
        except Exception as exc:
            # Avoid spamming the console on transient network/API issues.
            if time.time() - self._last_live_error_log > 30:
                print(f"⚠️ Live price fetch failed ({exc}); continuing with last known values.")
                self._last_live_error_log = time.time()

    def generate_trade(self, symbol: str, base_price: Optional[float] = None) -> dict:
        """Generate a trade event for a crypto symbol."""
        if base_price is None:
            base_price = self.current_prices.get(symbol)

        if base_price is None:
            # Absolute fallback if we somehow have no price yet.
            base_price = 1.0

        # Slight variation to mimic trade-to-trade noise.
        if symbol in self.live_symbol_map:
            volatility = 0.0008  # 0.08% jitter around the live price
        else:
            volatility = 0.003  # fallback for any simulated symbols

        price_change = random.uniform(-1, 1) * volatility * base_price
        new_price = max(base_price + price_change, 0.0001)
        self.current_prices[symbol] = new_price

        # Generate trade size based on symbol
        base_sizes = {
            'BTC/USDT': (0.001, 0.05),
            'ETH/USDT': (0.01, 0.5),
        }
        min_size, max_size = base_sizes.get(symbol, (1, 10))
        size = random.uniform(min_size, max_size)
        trade_volume = size * new_price
        self.volume_tracker[symbol] += trade_volume

        trade = {
            'symbol': symbol,
            'price': round(new_price, 4),
            'size': round(size, 4),
            'volume': round(trade_volume, 2),
            'timestamp': datetime.utcnow().isoformat(),
            'exchange': 'coingecko',
            'side': random.choice(['buy', 'sell'])
        }

        return trade
    
    def start_producing(self, interval=1.0):
        """Start generating and sending trade data"""
        print("🚀 Starting crypto data generator...")
        print(f"📊 Monitoring symbols: {CRYPTO_SYMBOLS}")
        
        try:
            while True:
                # Refresh live prices once per loop (roughly every `interval` seconds)
                self.fetch_live_prices()

                for symbol in CRYPTO_SYMBOLS:
                    trade = self.generate_trade(
                        symbol,
                        base_price=self.current_prices.get(symbol)
                    )
                    
                    # Send to Kafka
                    self.producer.produce(
                        TOPICS['trades'],
                        key=symbol,
                        value=json.dumps(trade),
                        callback=self.delivery_report
                    )
                
                # Wait for any outstanding messages to be delivered
                self.producer.flush()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping data generator...")
        finally:
            self.producer.flush()

if __name__ == "__main__":
    generator = CryptoDataGenerator()
    generator.start_producing()
