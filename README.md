# 🚀 Minimal Crypto Analytics Dashboard

| :---: |
| ![Dashboard demo](docs/output_animation.gif) |

A lightweight end-to-end pipeline that streams simulated and live-enriched cryptocurrency trades through Apache Kafka, computes rolling analytics, and renders them in a real-time Streamlit dashboard. It is intended as a reference implementation for experimenting with streaming data systems on a developer laptop.

## 📋 Features

- **Real-time Data Simulation**: Generates lifelike trades for BTC, ETH, and other assets with optional live price refreshes from CoinGecko
- **Streaming Analytics**: Aggregates trades into rolling metrics, anomaly alerts, and market health indicators in near real-time
- **Interactive Visualization**: Streamlit app with Plotly charts, market metrics, and alert feeds
- **Kafka-first Architecture**: Clear separation between producers, processors, and consumers for easy extension

## 🧱 Architecture

- `producers/data_generator.py`: Publishes synthetic trades (optionally refreshed with live prices) to Kafka
- `consumers/metrics_calculator.py`: Consumes trades, calculates rolling metrics, emits alerts, and republishes results
- `consumers/dashboard_consumer.py`: Buffers metrics and alerts for dashboard consumption
- `dashboard/app.py`: Streamlit UI that displays live KPIs, charts, and recent alerts
- `run.py`: Orchestrator that boots the generator, metrics processor, and dashboard once Kafka topics are available
- `docker-compose.yml`: Spins up a local Kafka + ZooKeeper stack and Kafka UI

## 🛠️ Tech Stack

- Python 3.10+
- Streamlit for the dashboard UI
- Plotly & pandas for visualization and data shaping
- Apache Kafka (via Confluent Platform images) for streaming
- confluent-kafka Python client for Kafka integration
- Docker Compose for local Kafka infrastructure

## 📦 Prerequisites

- Python 3.10 or newer available on your machine
- Docker and Docker Compose (to launch Kafka, ZooKeeper, and Kafka UI)
- (Optional) A `.env` file if you want to override defaults such as `KAFKA_BROKERS`

## 🧑‍💻 Local Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/<your-org>/minimal-crypto-dashboard.git
   cd minimal-crypto-dashboard
   ```

2. **Create and activate a virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

3. **Install Python dependencies**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Start Kafka services**
   ```bash
   docker compose up -d
   ```
   This launches ZooKeeper, Kafka, and an optional Kafka UI at `http://localhost:8080`.

5. **Run the orchestrator**
   ```bash
   python run.py
   ```
   The script ensures Kafka topics exist, starts the trade generator, metrics calculator, and launches the Streamlit dashboard on port `8501`.

6. **Explore the dashboard**
   - Dashboard UI: http://localhost:8501
   - Kafka UI (optional): http://localhost:8080

7. **Shut everything down**
   - Press `Ctrl+C` in the terminal running `python run.py`
   - Stop Kafka services when finished:
     ```bash
     docker compose down
     ```

## 🧰 Configuration

The default Kafka bootstrap server is `localhost:29092`. Override it (and other settings) by creating a `.env` file in the project root:

```
KAFKA_BROKERS=localhost:29092
```

Update `config.py` to adjust symbols, dashboard refresh intervals, or topic names.
